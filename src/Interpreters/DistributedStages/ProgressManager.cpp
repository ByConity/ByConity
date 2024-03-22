#include <Interpreters/DistributedStages/ProgressManager.h>
#include <common/logger_useful.h>

namespace DB
{
void ProgressManager::onProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_)
{
    std::unique_lock lock(segment_progress_mutex);
    auto instance_id = PlanSegmentInstanceId{segment_id, parallel_index};
    auto & p = segment_progress[instance_id];
    if (!p.is_final) /// if final progress has been accepted, ignore normal progress
    {
        this->progress.incrementPiecewiseAtomically(progress_);
        if (progress_callback)
            progress_callback(progress_);
        p.progress.incrementPiecewiseAtomically(progress_);
    }
}

void ProgressManager::onFinalProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_)
{
    auto instance_id = PlanSegmentInstanceId{segment_id, parallel_index};
    {
        std::unique_lock lock(segment_progress_mutex);
        auto & p = segment_progress[instance_id];
        p.final_progress.incrementPiecewiseAtomically(progress_);
        p.is_final = true;
    }
    if (progress_callback)
        progress_callback(getFinalProgressDiff({segment_id, parallel_index}));

    LOG_TRACE(
        log,
        "on final progress query_id:{} segment_id:{} parallel_index:{} progress:{}",
        query_id,
        instance_id.segment_id,
        instance_id.parallel_id,
        progress_.getValues().toString());
}

Progress ProgressManager::getFinalProgressDiff(PlanSegmentInstanceId instance_id) const
{
    ProgressValues diff;
    {
        std::unique_lock lock(segment_progress_mutex);
        Progress final_progress, past_progress;
        auto iter = segment_progress.find(instance_id);
        if (iter != segment_progress.end())
        {
            final_progress.incrementPiecewiseAtomically(iter->second.final_progress);
            past_progress.incrementPiecewiseAtomically(iter->second.progress);
            auto final_v = final_progress.getValues();
            auto past_v = past_progress.getValues();
            diff = final_v - past_v;
            if (past_v + diff != final_v)
                LOG_WARNING(
                    log,
                    "final progress seems wrong for query_id:{} final_progress:{} is expected to >= past_progress:{} the diff is:{}",
                    query_id,
                    final_v.toString(),
                    past_v.toString(),
                    diff.toString());
        }
    }
    Progress final_progress_diff(diff);
    return final_progress_diff;
}

Progress ProgressManager::getFinalProgress() const
{
    Progress final_progress;
    {
        std::unique_lock lock(segment_progress_mutex);
        for (const auto & s : segment_progress)
        {
            final_progress.incrementPiecewiseAtomically(s.second.final_progress);
        }
    }
    return final_progress;
}
}
