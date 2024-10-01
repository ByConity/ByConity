#include <atomic>
#include <memory>
#include <mutex>
#include <type_traits>

#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/MPPQueryCoordinator.h>
#include <Interpreters/DistributedStages/MPPQueryManager.h>
#include <Interpreters/DistributedStages/MPPQueryStatus.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/sendPlanSegment.h>
#include <common/logger_useful.h>
#include "Interpreters/DistributedStages/ProgressManager.h"


#include <boost/msm/front/euml/common.hpp>
#include <boost/msm/front/functor_row.hpp>
#include <boost/msm/front/state_machine_def.hpp>
#include <fmt/core.h>
#include <fmt/format.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

// query events

struct QueryErrorEvent
{
    bool cancel{false};
    QueryError query_error;
};

struct QueryDoneEvent
{
};

struct RootCauseErrorReceivedEvent
{
};

static const Int16 AMBIGUOS_ERROR_MAX_NUM = 10;

// Define mpp query coordinator FSM structure
class CoordinatorStateMachineDef : public boost::msm::front::state_machine_def<CoordinatorStateMachineDef>
{
public:
    MPPQueryCoordinator * coordinator_ptr;
    explicit CoordinatorStateMachineDef(MPPQueryCoordinator * coordinator_ptr_) : coordinator_ptr(coordinator_ptr_) { }

    // The list of FSM states
    struct StateInit : public boost::msm::front::state<>
    {
        // every (optional) entry/exit methods get the event passed.
        template <class Event, class FSM>
        void on_entry(Event const &, FSM & fsm)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "entering: StateInit");
        }
        template <class Event, class FSM>
        void on_exit(Event const &, FSM & fsm)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "leaving: StateInit");
        }
    };
    struct StateCancel : public boost::msm::front::state<>
    {
        template <class Event, class FSM>
        void on_entry(Event const & event, FSM & fsm)
        {
            fsm.coordinator_ptr->query_status.status_code.store(MPPQueryStatusCode::CANCEL, std::memory_order_release);
            LOG_TRACE(fsm.coordinator_ptr->log, "entering: StateCancel");
            fsm.coordinator_ptr->query_context->getPlanSegmentProcessList().tryCancelPlanSegmentGroup(fsm.coordinator_ptr->query_id);
            fsm.coordinator_ptr->query_context->getSegmentScheduler()->cancelPlanSegmentsFromCoordinator(
                fsm.coordinator_ptr->query_id, event.query_error.code, event.query_error.message, fsm.coordinator_ptr->query_context);
        }
        template <class Event, class FSM>
        void on_exit(Event const &, FSM & fsm)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "leaving: StateCancel");
        }
    };

    struct StateWaitRootCauseError : public boost::msm::front::state<>
    {
        template <class Event, class FSM>
        void on_entry(Event const &, FSM & fsm)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "entering: StateWaitRootCauseError");
            fsm.coordinator_ptr->query_status.status_code.store(MPPQueryStatusCode::WAIT_ROOT_ERROR, std::memory_order_release);
        }
        template <class Event, class FSM>
        void on_exit(Event const &, FSM & fsm)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "leaving: StateWaitRootCauseError");
            fsm.coordinator_ptr->status_cv.notify_all();
        }
    };

    struct StateFinish : public boost::msm::front::state<>
    {
        template <class Event, class FSM>
        void on_entry(Event const &, FSM & fsm)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "entering: StateFinish");
            fsm.coordinator_ptr->query_status.status_code.store(MPPQueryStatusCode::FINISH, std::memory_order_release);
        }
        template <class Event, class FSM>
        void on_exit(Event const &, FSM & fsm)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "leaving: StateFinish");
        }
    };

    // // the initial state of the Coordinator StateMachine. Must be defined
    using initial_state = StateInit;

    // transition actions
    struct StateCancelToWaitRootCauseError
    {
        template <class EVT, class FSM, class SourceState, class TargetState>
        void operator()(EVT const &, FSM & fsm, SourceState &, TargetState &)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "Cancel->WaitRootCauseError");
        }
    };
    struct StateInitToToFinish
    {
        template <class EVT, class FSM, class SourceState, class TargetState>
        void operator()(EVT const &, FSM & fsm, SourceState &, TargetState &)
        {
            LOG_TRACE(fsm.coordinator_ptr->log, "Init->Finish");
            std::unique_lock lock(fsm.coordinator_ptr->status_mutex);
            fsm.coordinator_ptr->query_status.success = true;
        }
    };

    using none = boost::msm::front::none;
    // Transition table for coodinator state machine
    struct transition_table : boost::mpl::vector< // NOLINT
                                  boost::msm::front::Row<StateInit, QueryDoneEvent, StateFinish>,
                                  boost::msm::front::Row<StateInit, QueryErrorEvent, StateCancel>,
                                  boost::msm::front::Row<StateCancel, none, StateWaitRootCauseError>,
                                  boost::msm::front::Row<StateWaitRootCauseError, RootCauseErrorReceivedEvent, StateFinish>>
    {
    };

    // Replaces the default no-transition response with noop
    template <class FSM, class Event>
    void no_transition(Event const &, [[maybe_unused]] FSM & fsm, [[maybe_unused]] int state)
    {
    }
};

using CoordinatorStateMachine = boost::msm::back::state_machine<CoordinatorStateMachineDef>;


// static char const * const state_names[] = {"StateInit", "StateCancel", "StateWaitRootCauseError", "StateFinish"};
// void printCurrentState(CoordinatorStateMachine const & p)
// {
//     std::cout << " -> " << state_names[p.current_state()[0]] << std::endl;
// }

MPPQueryCoordinator::MPPQueryCoordinator(
    std::unique_ptr<PlanSegmentTree> plan_segment_tree_, ContextMutablePtr query_context_, MPPQueryOptions options_)
    : query_context(std::move(query_context_))
    , options(std::move(options_))
    , plan_segment_tree(std::move(plan_segment_tree_))
    , query_id(query_context->getClientInfo().current_query_id)
    , log(&Poco::Logger::get("MPPQueryCoordinator"))
    , state_machine(std::make_unique<CoordinatorStateMachine>(this))
    , progress_manager(query_id)
{
}

BlockIO MPPQueryCoordinator::execute()
{
    state_machine->start();

    auto this_coordinator = shared_from_this();
    MPPQueryManager::instance().registerQuery(query_id, this_coordinator);

    PlanSegmentsStatusPtr scheduler_status;

    if (plan_segment_tree->getNodes().size() > 1)
    {
        RuntimeFilterManager::getInstance().registerQuery(query_id, *plan_segment_tree, query_context);
    }

    query_context->setCoordinatorAddress(getLocalAddress(*query_context));
    query_context->setPlanSegmentInstanceId(PlanSegmentInstanceId{0, 0});

    /// set progress_callback before send plan segment
    progress_manager.setProgressCallback([previous_progress_callback = query_context->getProgressCallback(),
                                          entry = query_context->getProcessListEntry()](const Progress & p) {
        if (previous_progress_callback)
            previous_progress_callback(p);
        if (auto process_list_elem_ptr = entry.lock())
            process_list_elem_ptr->get().updateProgressIn(p);
    });

    {
        /// only send progress before executing final plan segment,
        /// working thread will join when this tcp progress sender is destroyed
        auto sender = std::make_unique<TCPProgressSender>(
            query_context->getSendTCPProgress(), query_context->getSettingsRef().interactive_delay / 1000);
        scheduler_status = query_context->getSegmentScheduler()->insertPlanSegments(query_id, plan_segment_tree.get(), query_context);
    }

    if (scheduler_status && !scheduler_status->exception.empty())
    {
        throw Exception(
            "Query failed before final task execution, error message: " + scheduler_status->exception, scheduler_status->error_code);
    }

    if (!scheduler_status || !scheduler_status->is_final_stage_start)
    {
        throw Exception("Cannot get scheduler status from segment scheduler or final stage not started yet", ErrorCodes::LOGICAL_ERROR);
    }

    initializePostProcessingRPCReceived();

    auto * final_segment = plan_segment_tree->getRoot()->getPlanSegment();
    final_segment->update(query_context);
    LOG_TRACE(log, "EXECUTE\n" + final_segment->toString());

    auto final_segment_instance = std::make_unique<PlanSegmentInstance>();
    final_segment_instance->info = PlanSegmentExecutionInfo{.parallel_id = 0};
    final_segment_instance->info.execution_address = getLocalAddress(*query_context);
    final_segment_instance->plan_segment = std::make_unique<PlanSegment>(std::move(*final_segment));

    try
    {
        auto res = DB::lazyExecutePlanSegmentLocally(std::move(final_segment_instance), query_context);
        res.coordinator = this_coordinator;
        return res;
    }
    catch (const Exception & e)
    {
        if (isAmbiguosError(e.code()))
        {
            auto status = waitUntilFinish(e.code(), e.message());
            throw Exception(status.summarized_error_msg, status.error_code);
        }
        throw;
    }
}

SummarizedQueryStatus MPPQueryCoordinator::waitUntilFinish(int error_code, const String & error_msg)
{
    std::unique_lock lock(status_mutex);
    if (status_cv.wait_for(lock, std::chrono::milliseconds(query_context->getSettingsRef().distributed_query_wait_exception_ms), [this] {
            return this->query_status.status_code == MPPQueryStatusCode::FINISH;
        }))
    {
        if (query_status.success)
        {
            return SummarizedQueryStatus{.success = true};
        }
    }
    String summarized_error_msg;
    if (!query_status.root_cause_error.code)
    {

        query_status.root_cause_error = {.code = error_code, .message = error_msg};
        summarized_error_msg = fmt::format(
            "Query [{}] failed with RootCause: {}; \n AdditionalErrors: {} ",
            query_id,
            query_status.root_cause_error,
            fmt::join(query_status.additional_errors, "\n"));
    }
    else
    {
        summarized_error_msg = query_status.root_cause_error.message;
    }

    return SummarizedQueryStatus{
        .success = query_status.success,
        .cancelled = query_status.cancelled,
        .error_code = query_status.root_cause_error.code,
        .summarized_error_msg = std::move(summarized_error_msg)};
}

void MPPQueryCoordinator::updateSegmentInstanceStatus(const RuntimeSegmentStatus & status)
{
    LOG_TRACE(
        log,
        "updateSegmentInstanceStatus query_id:{} segment_id:{} is_succeed:{} is_cancelled:{} code:{} message:{}",
        query_id,
        status.segment_id,
        status.is_succeed,
        status.is_cancelled,
        status.code,
        status.message);
    if (status.is_succeed)
    {
        if (status.segment_id == 0)
        {
            triggerEvent(QueryDoneEvent());
        }
        return;
    }

    QueryError query_error{.code = status.code, .message = status.message, .segment_id = status.segment_id};

    tryUpdateRootErrorCause(query_error, status.is_cancelled);
}

void MPPQueryCoordinator::tryUpdateRootErrorCause(const QueryError & query_error, bool is_canceled)
{
    if (query_status.status_code.load(std::memory_order_acquire) == MPPQueryStatusCode::INIT)
    {
        triggerEvent(QueryErrorEvent{.cancel = is_canceled, .query_error = query_error});
    }

    std::unique_lock lock(status_mutex);
    if (query_status.success)
        return;

    if (isAmbiguosError(query_error.code))
    {
        if (query_status.additional_errors.size() < AMBIGUOS_ERROR_MAX_NUM)
            query_status.additional_errors.emplace_back(std::move(query_error));
        return;
    }

    if (!query_status.root_cause_error.code)
    {
        query_status.root_cause_error = std::move(query_error);
        lock.unlock();
        triggerEvent(RootCauseErrorReceivedEvent());
        return;
    }
    query_status.additional_errors.emplace_back(std::move(query_error));
}

template <class Event>
boost::msm::back::HandledEnum MPPQueryCoordinator::triggerEvent(Event const & evt)
{
    std::unique_lock lock(state_machine_mutex);
    return state_machine->process_event(evt);
}

void MPPQueryCoordinator::onProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_)
{
    progress_manager.onProgress(segment_id, parallel_index, progress_);
}

void MPPQueryCoordinator::onFinalProgress(UInt32 segment_id, UInt32 parallel_index, const Progress & progress_)
{
    progress_manager.onFinalProgress(segment_id, parallel_index, progress_);
    if (query_context->getSettingsRef().enable_wait_for_post_processing)
    {
        {
            std::unique_lock lock(post_processing_rpc_waiting_mutex);
            PlanSegmentInstanceId instance_id{segment_id, parallel_index};
            // save instance id in post_processing_rpc_waiting if not initialized
            if (post_processing_rpc_waiting_initialized)
            {
                post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].erase(instance_id);
            }
            else
            {
                post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].insert(instance_id);
            }
        }
        post_processing_rpc_waiting_cv.notify_all();
    }
}

Progress MPPQueryCoordinator::getFinalProgress() const
{
    return progress_manager.getFinalProgress();
}

void MPPQueryCoordinator::initializePostProcessingRPCReceived()
{
    if (query_context->getSettingsRef().enable_wait_for_post_processing)
    {
        {
            std::unique_lock lock(post_processing_rpc_waiting_mutex);
            auto instance_ids = query_context->getSegmentScheduler()->getIOPlanSegmentInstanceIDs(query_id);
            // remove instance id which has been received before
            for (auto instance_id : post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost])
            {
                if (instance_ids.find(instance_id) != instance_ids.end())
                    instance_ids.erase(instance_id);
            }
            LOG_INFO(log, "initializePostProcessingRPCReceived query_id:{} with {} instances", query_id, instance_ids.size());
            post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost] = std::move(instance_ids);
            post_processing_rpc_waiting_initialized = true;
        }
        post_processing_rpc_waiting_cv.notify_all();
    }
}

void MPPQueryCoordinator::waitUntilAllPostProcessingRPCReceived()
{
    // if setting is not enabled, just skip wait
    if (!query_context->getSettingsRef().enable_wait_for_post_processing)
        return;
    std::unique_lock lock(post_processing_rpc_waiting_mutex);
    bool need_wait = false;

    if (!post_processing_rpc_waiting_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "post_processing_rpc not initialized for query_id:{}", query_id);
    // need to wait if not all already received
    if (!post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].empty())
        need_wait = true;

    if (!need_wait)
    {
        LOG_TRACE(log, "waitUntilAllPostProcessingRPCReceived no need to wait");
        return;
    }

    if (!post_processing_rpc_waiting_cv.wait_for(
            lock, std::chrono::milliseconds(query_context->getSettingsRef().wait_for_post_processing_timeout_ms), [this] {
                return post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].empty();
            }))
    {
        std::stringstream not_received_msg;
        for (auto instance_id : post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost])
        {
            not_received_msg << instance_id.toString();
        }
        LOG_WARNING(log, fmt::format("waitUntilAllPostProcessingRPCReceived failed for {} timeout, empty:{}", not_received_msg.str(), post_processing_rpc_waiting[PostProcessingRPCID::ReportPlanSegmentCost].empty()));
    }
    LOG_TRACE(log, "waitUntilAllPostProcessingRPCReceived done");
}

MPPQueryCoordinator::~MPPQueryCoordinator()
{
    //TODO: refine as QueryComponents
    try
    {
        RuntimeFilterManager::getInstance().removeQuery(query_id);
        query_context->getSegmentScheduler()->finishPlanSegments(query_id);
        if (query_context->getSettingsRef().bsp_mode)
        {
            query_context->getExchangeDataTracker()->unregisterExchanges(query_id);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("~MPPQueryCoordinator exception for query_id:{}", query_id));
    }

    MPPQueryManager::instance().clearQuery(query_id);
}

}
