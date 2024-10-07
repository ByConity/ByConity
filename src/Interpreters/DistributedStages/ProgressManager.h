#pragma once

#include <Common/Logger.h>
#include <condition_variable>
#include <memory>
#include <IO/Progress.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>

namespace DB
{

// send progress repeatedly
class TCPProgressSender
{
public:
    TCPProgressSender(std::function<void()> send_tcp_progress_, size_t interval_);
    ~TCPProgressSender();

private:
    LoggerPtr logger;
    std::atomic_bool shutdown = {false};
    std::mutex mu;
    std::condition_variable var;
    std::function<void()> send_tcp_progress;
    std::unique_ptr<ThreadFromGlobalPool> thread;
    size_t interval;
};

class ProgressManager
{
public:
    explicit ProgressManager(const String & query_id_) : log(getLogger("ProgressManager")), query_id(query_id_)
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
    LoggerPtr log;
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
