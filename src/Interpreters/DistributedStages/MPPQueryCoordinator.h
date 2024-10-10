#pragma once
#include <Common/Logger.h>
#include <memory>
#include <mutex>
#include <string_view>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/MPPQueryStatus.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/ProgressManager.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Processors/Formats/OutputStreamToOutputFormat.h>
#include <boost/core/noncopyable.hpp>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <common/types.h>

#include <boost/msm/back/state_machine.hpp>
namespace DB
{
class PlanSegmentTree;
class CoordinatorStateMachineDef;
struct BlockIO;

enum PostProcessingRPCID : uint8_t
{
    ReportPlanSegmentCost = 0
};

struct MPPQueryOptions
{
    bool need_all_instance_result{false};
};

class MPPQueryCoordinator final: public std::enable_shared_from_this<MPPQueryCoordinator>, private boost::noncopyable
{
public:
    MPPQueryCoordinator(std::unique_ptr<PlanSegmentTree> plan_segment_tree_, ContextMutablePtr query_context_, MPPQueryOptions options_);

    BlockIO execute();

    SummarizedQueryStatus waitUntilFinish(int error_code, const String & error_msg);

    //TODO: redefine RuntimeSegmentsStatus
    void updateSegmentInstanceStatus(const RuntimeSegmentStatus & status);

    /// normal progress received from sendProgress rpc
    void onProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_);
    /// final progress received from updatePlanSegmentStatus
    void onFinalProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_);
    /// final progress is the last progress received from worker instance, and is assumed to contain all past progress
    Progress getFinalProgress() const;

    /// initialize post_processing_rpc_waiting, including all plan segments except the final plan segment.
    void initializePostProcessingRPCReceived();
    /// wait unitl all post processing rpcs have been received.
    void waitUntilAllPostProcessingRPCReceived();

    void tryUpdateRootErrorCause(const QueryError & query_error, bool is_canceled);

    ContextPtr getContext() { return query_context; }

    ~MPPQueryCoordinator();

    UInt64 getNormalizedQueryPlanHash() const
    {
        return normalized_query_plan_hash;
    }

private:
    friend class CoordinatorStateMachineDef;

    template <class Event>
    boost::msm::back::HandledEnum triggerEvent(Event const & evt); // It use state_machine_mutex;

    ContextMutablePtr query_context;
    MPPQueryOptions options;
    std::shared_ptr<PlanSegmentTree> plan_segment_tree;
    const String & query_id;
    LoggerPtr log;

    // All allowed lock order: (state_machine_mutex,status_mutex) or (status_mutex) or (state_machine_mutex)
    mutable bthread::Mutex state_machine_mutex;
    std::unique_ptr<boost::msm::back::state_machine<CoordinatorStateMachineDef>> state_machine;

    mutable bthread::Mutex status_mutex;
    bthread::ConditionVariable status_cv;
    MPPQueryStatus query_status;

    mutable bthread::Mutex post_processing_rpc_waiting_mutex;
    bthread::ConditionVariable post_processing_rpc_waiting_cv;
    std::unordered_map<PostProcessingRPCID, PlanSegmentSet> post_processing_rpc_waiting = {};
    bool post_processing_rpc_waiting_initialized = false;

    ProgressManager progress_manager;
    UInt64 normalized_query_plan_hash = 0;
};

using MPPQueryCoordinatorPtr = std::shared_ptr<MPPQueryCoordinator>;
}
