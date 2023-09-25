#pragma once
#include <memory>
#include <mutex>
#include <string_view>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/MPPQueryStatus.h>
#include <boost/core/noncopyable.hpp>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <common/types.h>
#include "Interpreters/DistributedStages/PlanSegmentExecutor.h"

#include <boost/msm/back/state_machine.hpp>
namespace DB
{
class PlanSegmentTree;
class CoordinatorStateMachineDef;
struct BlockIO;

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
    void updateSegmentInstanceStatus(RuntimeSegmentsStatus status);

    ContextPtr getContext() { return query_context; };

    ~MPPQueryCoordinator();

private:
    friend class CoordinatorStateMachineDef;

    template <class Event>
    boost::msm::back::HandledEnum triggerEvent(Event const & evt); // It use state_machine_mutex;

    ContextMutablePtr query_context;
    MPPQueryOptions options;
    std::shared_ptr<PlanSegmentTree> plan_segment_tree;
    const String & query_id;
    Poco::Logger * log;

    // All allowed lock order: (state_machine_mutex,status_mutex) or (status_mutex) or (state_machine_mutex)
    mutable bthread::Mutex state_machine_mutex;
    std::unique_ptr<boost::msm::back::state_machine<CoordinatorStateMachineDef>> state_machine;

    mutable bthread::Mutex status_mutex;
    bthread::ConditionVariable status_cv;
    MPPQueryStatus query_status;
};

}
