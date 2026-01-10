// Copyright 2023-2025 DreamWorks Animation LLC
// SPDX-License-Identifier: Apache-2.0

#include "RenderContextDriver.h"

#include <arras4_log/LogEventStream.h>
#include <mcrt_dataio/share/util/BandwidthTracker.h>
#include <mcrt_dataio/share/util/SysUsage.h>
#include <message_api/Address.h>
#include <moonray/rendering/rndr/PathVisualizerManager.h>
#include <moonray/rendering/rndr/RenderContext.h>

//#define DEBUG_LOG_MESSAGE

namespace mcrt_computation {

void 
RenderContextDriver::startFrame()
{
    mFeedbackActiveRuntime = mFeedbackActiveUserInput; // setup render time feedback flag from user defined flag
    if (mFeedbackActiveRuntime || mProgressiveFrameRecMode) {
        //
        // Start feedback operation
        //
        // Runtime feedback condition always switches at START-FRAME timing.
        // This is required due to feedback internal data being delta-coded information and
        // we can not activate feedback logic middle of rendering.
        // 
        mSentImageCache.reset(scene_rdl2::math::convertToClosedViewport(mViewport));
    }

    //------------------------------

    if (mStartFrameCallBack) mStartFrameCallBack(mReloadingScene, mSource);

    {
        std::lock_guard<std::mutex> lock(mMutexMcrtNodeInfoMapItem);
        mcrt_dataio::McrtNodeInfo &mcrtNodeInfo = mMcrtNodeInfoMapItem.getMcrtNodeInfo();
        mcrtNodeInfo.setRenderActive(true); // set renderActive = on
        mcrtNodeInfo.setSyncId(mSyncId); // update syncId
        mcrtNodeInfo.setRenderPrepStatsInit(); // reset renderPrepStats to initial condition
    }

    // Setup call back function regarding renderPrep status report to the downstream.
    moonray::rndr::RenderContext *renderContext = getRenderContext();
    renderContext->setRenderPrepCallBack(
        [&](const scene_rdl2::grid_util::RenderPrepStats &rPrepStats) { // RenderPrepStatsCallBack function
            // We only update mcrtNodeInfo by RenderPrepStats. send operation should be done by arras main thread
            std::lock_guard<std::mutex> lock(mMutexMcrtNodeInfoMapItem);            
            mcrt_dataio::McrtNodeInfo &mcrtNodeInfo = mMcrtNodeInfoMapItem.getMcrtNodeInfo();
            mcrtNodeInfo.setRenderPrepStats(rPrepStats);

            if (rPrepStats.isCompleted()) {
                if (mGenLineCondition == PathVisGenLineCondition::WAIT_RENDERPREP_FINISHED) {
                    mGenLineCondition = PathVisGenLineCondition::RUN_GENLINE;
                }

                scene_rdl2::math::Vec3f focusPoint;
                if (computeOrbitCamAutoFocusPoint(focusPoint)) {
                    /* for debug
                    std::cerr << ">> RenderContextDriver_startStop.cc updateOrbitCamAutoFocusPoint() {\n"
                              << "  focusPoint:" << focusPoint[0] << ", " << focusPoint[1] << ", " << focusPoint[2] << '\n'
                              << "}\n";
                    */
                    mOrbitCamAutoFocusPoint = focusPoint;
                }
            }
        },
        [&]() -> bool {
            return (*mRenderPrepCancel); // true:cancel_renderPrep false:not_cancel
        });

    if (mTimingRecFrame) mTimingRecFrame->setRenderPrepStartTiming();

    if (isPathVisualizerMode()) {
        moonray::rndr::PathVisualizerManager* visMgrObsrPtr = mRenderContext->getPathVisualizerManager().get();
        if (visMgrObsrPtr->isInStartRecordState()) { // request simulation mode rendering 
            // We need to change Path Visualizer's state to RECORD before waking up the Renderprep thread.
            visMgrObsrPtr->setRecordState();
        }
    }

    start(); // renderContextDriver thread executes renderPrep and fbSender setup
}

bool
RenderContextDriver::computeOrbitCamAutoFocusPoint(scene_rdl2::math::Vec3f& focusPoint)
{
    const scene_rdl2::math::HalfOpenViewport vp = getRenderContext()->getRezedRegionWindow();
    const int centerX = vp.width() / 2;
    const int centerY = vp.height() / 2;

    // must use offset between center point of aperture window and center point
    // of region window so that the region window is centered on the pick point.
    const scene_rdl2::math::HalfOpenViewport avp = getRenderContext()->getRezedApertureWindow();
    const scene_rdl2::math::HalfOpenViewport rvp = getRenderContext()->getRezedRegionWindow();
    const int offsetX = (avp.max().x + avp.min().x) / 2 - (rvp.max().x + rvp.min().x) / 2;
    const int offsetY = (avp.max().y + avp.min().y) / 2 - (rvp.max().y + rvp.min().y) / 2;

    if (!getRenderContext()->handlePickLocation(centerX + offsetX, centerY - offsetY, &focusPoint)) {
        return false; // no hit
    }
    return true;
}

bool
RenderContextDriver::stopFrame()
// return status which is just before execute stopFrame operation
// return true : renderContext was rendering condition
//        false : renderContext was stop condition
{
    if (!mRenderContext || !mRenderContext->isFrameRendering()) {
        // mRenderContext->isFrameRendering() returns true when MCRT stage started

        if (!mRenderContext->isRenderPrepRun()) {
            // We are not within renderPrep phase. We don't need renderPrepCancel operation
            if (mRenderPrepCancel) (*mRenderPrepCancel) = false; // reset renderPrep cancel condition
            {
                std::lock_guard<std::mutex> lock(mMutexMcrtNodeInfoMapItem);
                if (mRenderPrepCancel) {
                    mMcrtNodeInfoMapItem.getMcrtNodeInfo().setRenderPrepCancel((*mRenderPrepCancel));
                }
            }
        }
        return false;
    }

#   ifdef  DEBUG_LOG_MESSAGE
    ARRAS_LOG_INFO("stopRender sequence start");
#   endif // end DEBUG_LOG_MESSAGE

    ARRAS_LOG_DEBUG("Stopping frame...");
    mRenderContext->stopFrame();
    if (mRenderPrepCancel) (*mRenderPrepCancel) = false; // reset renderPrep cancel condition
    {
        std::lock_guard<std::mutex> lock(mMutexMcrtNodeInfoMapItem);
        if (mRenderPrepCancel) {
            mMcrtNodeInfoMapItem.getMcrtNodeInfo().setRenderPrepCancel((*mRenderPrepCancel));
        }
        mMcrtNodeInfoMapItem.getMcrtNodeInfo().setRenderActive(false); // set renderActive = off
    }

#   ifdef  DEBUG_LOG_MESSAGE
    ARRAS_LOG_INFO("stopRender sequence done");
#   endif // end DEBUG_LOG_MESSAGE

    if (mStopFrameCallBack) mStopFrameCallBack(mSource);
    
    return true;
}

void
RenderContextDriver::requestStopAtPassBoundary(const uint32_t syncId)
{
    if (mSyncId != syncId || !mRenderContext) return; // early exit

    if (mRenderContext->isFrameRendering() && !mRenderContext->isFrameComplete()) {
        // Set request stopAtPassBoundary if we are rendering
        mRenderContext->requestStopRenderAtPassBoundary();
        ARRAS_LOG_INFO("RenderComplete sequence start : requested stop_at_pass_boundary");
    }
}

void
RenderContextDriver::reconstructSceneFromBackup()
//
// This function reconstructs renderContext from scratch based on the backup version of sceneContext.
//    
{
    moonray::rndr::RenderContext *renderContext = resetRenderContext(); // full reset of sceneContext

    scene_rdl2::rdl2::BinaryWriter w(*mSceneContextBackup);
    w.setDeltaEncoding(false);

    std::string manifest;
    std::string payload;
    w.toBytes(manifest, payload);
    
    mReloadingScene = true;
    renderContext->updateScene(manifest, payload);

    std::stringstream initMessages;
    renderContext->initialize(initMessages); // renderLayer (mLayer) is updated here
}

} // namespace mcrt_computation
