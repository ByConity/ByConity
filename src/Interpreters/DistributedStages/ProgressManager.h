#pragma once

#include <IO/Progress.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>

namespace DB
{
class ProgressManager
{
public:
    explicit ProgressManager(const String & query_id_) : log(&Poco::Logger::get("ProgressManager")), query_id(query_id_)
    {
    }
    /// normal progress received from sendProgress rpc
    void onProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_);
    /// final progress received from updatePlanSegmentStatus
    void onFinalProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_);
    /// final progress is the last progress received from worker instance, and is assumed to contain all past progress
    Progress getFinalProgress() const;
    void setProgressCallback(ProgressCallback progress_callback_)
    {
        progress_callback = std::move(progress_callback_);
    }

private:
    Poco::Logger * log;
    String query_id;
    /// only collects progress in worker segments
    Progress progress;
    struct SegmentProgress
    {
        Progress progress;
        Progress final_progress;
        bool is_final = false;
    };
    std::unordered_map<PlanSegmentInstanceId, SegmentProgress> segment_progress;
    mutable bthread::Mutex segment_progress_mutex;
    ProgressCallback progress_callback = nullptr;

    Progress getFinalProgressDiff(PlanSegmentInstanceId instance_id) const;
};
}
