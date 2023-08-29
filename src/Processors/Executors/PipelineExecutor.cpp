/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <queue>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <Processors/printPipeline.h>
#include <Common/EventCounter.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/MemoryTracker.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/printPipeline.h>
#include <Processors/ISource.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Protos/plan_segment_manager.pb.h>
#include <common/scope_guard_safe.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>

#ifndef NDEBUG
    #include <Common/Stopwatch.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXPIRED;
    extern const int QUERY_WAS_CANCELLED;
}

static bool checkCanAddAdditionalInfoToException(const DB::Exception & exception)
{
    /// Don't add additional info to limits and quota exceptions, and in case of kill query (to pass tests).
    return exception.code() != ErrorCodes::TOO_MANY_ROWS_OR_BYTES
           && exception.code() != ErrorCodes::QUOTA_EXPIRED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED;
}

PipelineExecutor::PipelineExecutor(Processors & processors_, QueryStatus * elem, const PipelineExecutorOptions & executor_options)
    : processors(processors_)
    , need_processors_profiles(executor_options.need_processors_profiles)
    , report_processors_profile(executor_options.report_processors_profile)
    , cancelled(false)
    , finished(false)
    , num_processing_executors(0)
    , expand_pipeline_task(nullptr)
    , process_list_element(elem)
{
    if (process_list_element)
    {
        report_processors_profile = process_list_element->getContext()->getSettingsRef().report_processors_profiles;
        need_processors_profiles = process_list_element->getContext()->getSettingsRef().log_processors_profiles || report_processors_profile;
    }

    try
    {
        graph = std::make_unique<ExecutingGraph>(processors);
    }
    catch (Exception & exception)
    {
        /// If exception was thrown while pipeline initialization, it means that query pipeline was not build correctly.
        /// It is logical error, and we need more information about pipeline.
        WriteBufferFromOwnString buf;
        printPipeline(processors, buf);
        buf.finalize();
        exception.addMessage("Query pipeline:\n" + buf.str());

        throw;
    }

    if (process_list_element)
    {
        // Add the pipeline to the QueryStatus at the end to avoid issues if other things throw
        // as that would leave the executor "linked"
        process_list_element->addPipelineExecutor(this);
    }
}

PipelineExecutor::~PipelineExecutor()
{
    if (process_list_element)
        process_list_element->removePipelineExecutor(this);

}

void PipelineExecutor::addChildlessProcessorsToStack(Stack & stack)
{
    UInt64 num_processors = processors.size();
    for (UInt64 proc = 0; proc < num_processors; ++proc)
    {
        if (graph->nodes[proc]->direct_edges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executed in single thread
            graph->nodes[proc]->status = ExecutingGraph::ExecStatus::Preparing;
        }
    }
}

static void executeJob(IProcessor * processor)
{
    try
    {
        processor->work();
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + processor->getName());
        throw;
    }
}

void PipelineExecutor::addJob(ExecutingGraph::Node * execution_state)
{
    auto job = [execution_state]()
    {
        try
        {
            // Stopwatch watch;
            executeJob(execution_state->processor);
            // execution_state->execution_time_ns += watch.elapsed();

            ++execution_state->num_executed_jobs;
        }
        catch (...)
        {
            execution_state->exception = std::current_exception();
        }
    };

    execution_state->job = std::move(job);
}

bool PipelineExecutor::expandPipeline(Stack & stack, UInt64 pid)
{
    auto & cur_node = *graph->nodes[pid];
    Processors new_processors;

    try
    {
        new_processors = cur_node.processor->expandPipeline();
    }
    catch (...)
    {
        cur_node.exception = std::current_exception();
        return false;
    }

    {
        std::lock_guard guard(processors_mutex);
        processors.insert(processors.end(), new_processors.begin(), new_processors.end());
    }

    uint64_t num_processors = processors.size();
    std::vector<uint64_t> back_edges_sizes(num_processors, 0);
    std::vector<uint64_t> direct_edge_sizes(num_processors, 0);

    for (uint64_t node = 0; node < graph->nodes.size(); ++node)
    {
        direct_edge_sizes[node] = graph->nodes[node]->direct_edges.size();
        back_edges_sizes[node] = graph->nodes[node]->back_edges.size();
    }

    auto updated_nodes = graph->expandPipeline(processors);

    for (auto updated_node : updated_nodes)
    {
        auto & node = *graph->nodes[updated_node];

        size_t num_direct_edges = node.direct_edges.size();
        size_t num_back_edges = node.back_edges.size();

        std::lock_guard guard(node.status_mutex);

        for (uint64_t edge = back_edges_sizes[updated_node]; edge < num_back_edges; ++edge)
            node.updated_input_ports.emplace_back(edge);

        for (uint64_t edge = direct_edge_sizes[updated_node]; edge < num_direct_edges; ++edge)
            node.updated_output_ports.emplace_back(edge);

        if (node.status == ExecutingGraph::ExecStatus::Idle)
        {
            node.status = ExecutingGraph::ExecStatus::Preparing;
            stack.push(updated_node);
        }
    }

    return true;
}

bool PipelineExecutor::tryAddProcessorToStackIfUpdated(ExecutingGraph::Edge & edge, Queue & queue, Queue & async_queue, size_t thread_number)
{
    /// In this method we have ownership on edge, but node can be concurrently accessed.

    auto & node = *graph->nodes[edge.to];

    std::unique_lock lock(node.status_mutex);

    ExecutingGraph::ExecStatus status = node.status;

    if (status == ExecutingGraph::ExecStatus::Finished)
        return true;

    if (edge.backward)
        node.updated_output_ports.push_back(edge.output_port_number);
    else
        node.updated_input_ports.push_back(edge.input_port_number);

    if (status == ExecutingGraph::ExecStatus::Idle)
    {
        node.status = ExecutingGraph::ExecStatus::Preparing;
        return prepareProcessor(edge.to, thread_number, queue, async_queue, std::move(lock));
    }
    else
        graph->nodes[edge.to]->processor->onUpdatePorts();

    return true;
}

bool PipelineExecutor::prepareProcessor(UInt64 pid, size_t thread_number, Queue & queue, Queue & async_queue, std::unique_lock<std::mutex> node_lock)
{
    /// In this method we have ownership on node.
    auto & node = *graph->nodes[pid];

    bool need_expand_pipeline = false;

    std::vector<ExecutingGraph::Edge *> updated_back_edges;
    std::vector<ExecutingGraph::Edge *> updated_direct_edges;

    {
#ifndef NDEBUG
        Stopwatch watch;
#endif

        std::unique_lock<std::mutex> lock(std::move(node_lock));

        try
        {
            // node.last_processor_status = node.processor->prepare(node.updated_input_ports, node.updated_output_ports);
            auto & processor = *node.processor;
            IProcessor::Status last_status = node.last_processor_status;
            IProcessor::Status status = processor.prepare(node.updated_input_ports, node.updated_output_ports);
            node.last_processor_status = status;

            if (need_processors_profiles)
            {
                /// NeedData
                if (last_status != IProcessor::Status::NeedData && status == IProcessor::Status::NeedData)
                {
                    processor.input_wait_watch.restart();
                }
                else if (last_status == IProcessor::Status::NeedData && status != IProcessor::Status::NeedData)
                {
                    processor.input_wait_elapsed_us += processor.input_wait_watch.elapsedMicroseconds();
                }

                /// PortFull
                if (last_status != IProcessor::Status::PortFull && status == IProcessor::Status::PortFull)
                {
                    processor.output_wait_watch.restart();
                }
                else if (last_status == IProcessor::Status::PortFull && status != IProcessor::Status::PortFull)
                {
                    processor.output_wait_elapsed_us += processor.output_wait_watch.elapsedMicroseconds();
                }
            }
            //LOG_TRACE(log, "prepare processor: {}, {}, {}, {}", pid, node.processor->getName(), thread_number, IProcessor::statusToName(node.last_processor_status));
        }
        catch (...)
        {
            node.exception = std::current_exception();
            return false;
        }

#ifndef NDEBUG
        node.preparation_time_ns += watch.elapsed();
#endif

        node.updated_input_ports.clear();
        node.updated_output_ports.clear();

        switch (node.last_processor_status)
        {
            case IProcessor::Status::NeedData:
            case IProcessor::Status::PortFull:
            {
                node.status = ExecutingGraph::ExecStatus::Idle;
                break;
            }
            case IProcessor::Status::Finished:
            {
                node.status = ExecutingGraph::ExecStatus::Finished;
                if (report_processors_profile)
                    reportProcessorProfile(node.processor);
                break;
            }
            case IProcessor::Status::Ready:
            {
                node.status = ExecutingGraph::ExecStatus::Executing;
                queue.push(&node);
                break;
            }
            case IProcessor::Status::Async:
            {
                node.status = ExecutingGraph::ExecStatus::Executing;
                async_queue.push(&node);
                break;
            }
            case IProcessor::Status::ExpandPipeline:
            {
                need_expand_pipeline = true;
                break;
            }
        }

        {
            for (auto & edge_id : node.post_updated_input_ports)
            {
                auto * edge = static_cast<ExecutingGraph::Edge *>(edge_id);
                updated_back_edges.emplace_back(edge);
                edge->update_info.trigger();
            }

            for (auto & edge_id : node.post_updated_output_ports)
            {
                auto * edge = static_cast<ExecutingGraph::Edge *>(edge_id);
                updated_direct_edges.emplace_back(edge);
                edge->update_info.trigger();
            }

            node.post_updated_input_ports.clear();
            node.post_updated_output_ports.clear();
        }
    }

    {
        for (auto & edge : updated_direct_edges)
        {
            if (!tryAddProcessorToStackIfUpdated(*edge, queue, async_queue, thread_number))
                return false;
        }

        for (auto & edge : updated_back_edges)
        {
            if (!tryAddProcessorToStackIfUpdated(*edge, queue, async_queue, thread_number))
                return false;
        }
    }

    if (need_expand_pipeline)
    {
        Stack stack;

        executor_contexts[thread_number]->task_list.emplace_back(&node, &stack);

        ExpandPipelineTask * desired = &executor_contexts[thread_number]->task_list.back();
        ExpandPipelineTask * expected = nullptr;

        while (!expand_pipeline_task.compare_exchange_strong(expected, desired))
        {
            if (!doExpandPipeline(expected, true))
                return false;

            expected = nullptr;
        }

        if (!doExpandPipeline(desired, true))
            return false;

        /// Add itself back to be prepared again.
        stack.push(pid);

        while (!stack.empty())
        {
            auto item = stack.top();
            if (!prepareProcessor(item, thread_number, queue, async_queue, std::unique_lock<std::mutex>(graph->nodes[item]->status_mutex)))
                return false;

            stack.pop();
        }
    }

    return true;
}

bool PipelineExecutor::doExpandPipeline(ExpandPipelineTask * task, bool processing)
{
    std::unique_lock lock(task->mutex);

    if (processing)
        ++task->num_waiting_processing_threads;

    task->condvar.wait(lock, [&]()
    {
        return task->num_waiting_processing_threads >= num_processing_executors || expand_pipeline_task != task;
    });

    bool result = true;

    /// After condvar.wait() task may point to trash. Can change it only if it is still in expand_pipeline_task.
    if (expand_pipeline_task == task)
    {
        result = expandPipeline(*task->stack, task->node_to_expand->processors_id);

        expand_pipeline_task = nullptr;

        lock.unlock();
        task->condvar.notify_all();
    }

    return result;
}

static UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

static UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

void collectProfileMetricRequest(
    Protos::ReportProcessorProfileMetricRequest & request,
    const AddressInfo current_address,
    const IProcessor * processor,
    const String & query_id,
    const std::chrono::time_point<std::chrono::system_clock> finish_time,
    const size_t segment_id = 0)
{
    auto get_proc_id = [](const IProcessor & proc) -> UInt64 { return reinterpret_cast<std::uintptr_t>(&proc); };

    std::vector<UInt64> parents;
    for (const auto & port : processor->getOutputs())
    {
        if (!port.isConnected())
            continue;
        const IProcessor & next = port.getInputPort().getProcessor();
        parents.push_back(get_proc_id(next));
    }

    request.set_query_id(query_id);
    request.set_event_time(time_in_seconds(finish_time));
    request.set_event_time_microseconds(time_in_microseconds(finish_time));
    request.set_id(get_proc_id(*processor));
    for (auto & parent_id : parents)
    {
        request.add_parent_ids(parent_id);
    }
    request.set_plan_step(reinterpret_cast<std::uintptr_t>(processor->getQueryPlanStep()));
    uint64_t count = processor->getWorkCount();
    request.set_plan_group(processor->getQueryPlanStepGroup() | (segment_id << 16) | (count << 32));

    request.set_processor_name(processor->getName());

    request.set_elapsed_us(processor->getElapsedUs());
    request.set_input_wait_elapsed_us(processor->getInputWaitElapsedUs());
    request.set_output_wait_elapsed_us(processor->getOutputWaitElapsedUs());

    auto stats = processor->getProcessorDataStats();
    request.set_input_rows(stats.input_rows);
    request.set_input_bytes(stats.input_bytes);
    request.set_output_rows(stats.output_rows);
    request.set_output_bytes(stats.output_bytes);
    request.set_step_id(processor->getStepId());
    request.set_worker_address(extractExchangeStatusHostPort(current_address));
}

void reportToCoordinator(
    const AddressInfo & coordinator_address,
    const AddressInfo & current_address,
    const IProcessor * processor,
    const String & query_id,
    const std::chrono::time_point<std::chrono::system_clock> finish_time,
    const size_t segment_id = 0)
{
    try
    {
        auto address = extractExchangeStatusHostPort(coordinator_address);
        std::shared_ptr<RpcClient> rpc_client = RpcChannelPool::getInstance().getClient(address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
        Protos::PlanSegmentManagerService_Stub manager(&rpc_client->getChannel());
        brpc::Controller cntl;
        Protos::ReportProcessorProfileMetricRequest request;
        Protos::ReportProcessorProfileMetricResponse response;
        collectProfileMetricRequest(request, current_address, processor, query_id, finish_time, segment_id);
        manager.reportProcessorProfileMetrics(&cntl, &request, &response, nullptr);
        rpc_client->assertController(cntl);
        LOG_TRACE(&Poco::Logger::get("PipelineExecutor"), "Processor-{} send profile metrics to coordinator successfully.", request.id());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void reportToCoordinator(
    const AddressInfo & coordinator_address,
    const AddressInfo & current_address,
    const Processors & processors,
    const String & query_id,
    const std::chrono::time_point<std::chrono::system_clock> finish_time,
    const size_t segment_id = 0)
{
    try
    {
        auto address = extractExchangeStatusHostPort(coordinator_address);
        std::shared_ptr<RpcClient> rpc_client = RpcChannelPool::getInstance().getClient(address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
        Protos::PlanSegmentManagerService_Stub manager(&rpc_client->getChannel());
        brpc::Controller cntl;
        Protos::BatchReportProcessorProfileMetricRequest requests;
        Protos::ReportProcessorProfileMetricResponse response;
        requests.set_query_id(query_id);
        for (const auto & processor : processors)
        {
            auto * request_ptr = requests.add_request();
            collectProfileMetricRequest(*request_ptr, current_address, processor.get(), query_id, finish_time, segment_id);
        }
        manager.batchReportProcessorProfileMetrics(&cntl, &requests, &response, nullptr);
        rpc_client->assertController(cntl);
        LOG_TRACE(&Poco::Logger::get("PipelineExecutor"), "Batch processors send profile metrics to coordinator successfully.");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void PipelineExecutor::reportProcessorProfileOnCancel(const Processors & processors_) const
{
    String query_id;
    size_t segment_id = 0;
    AddressInfo coordinator_address;
    AddressInfo current_address;
    if (process_list_element)
    {
        query_id = process_list_element->getClientInfo().initial_query_id;
        auto weak_segment_process_list_entry = process_list_element->getContext()->getPlanSegmentProcessListEntry().lock();
        if (weak_segment_process_list_entry)
        {
            PlanSegmentProcessList::EntryPtr segment_process_list_entry = std::move(weak_segment_process_list_entry);
            segment_id = segment_process_list_entry->getSegmentId();
            coordinator_address = segment_process_list_entry->getCoordinatorAddress();
            current_address = segment_process_list_entry->getCurrentAddress();
        }

        auto finish_time = std::chrono::system_clock::now();
        if (segment_id > 0)
            reportToCoordinator(coordinator_address, current_address, processors_, query_id, finish_time, segment_id);
    }
}

void PipelineExecutor::reportProcessorProfile(const IProcessor * processor) const
{
    String query_id;
    size_t segment_id = 0;
    AddressInfo coordinator_address;
    AddressInfo current_address;
    if (process_list_element)
    {
        query_id = process_list_element->getClientInfo().initial_query_id;
        auto weak_segment_process_list_entry = process_list_element->getContext()->getPlanSegmentProcessListEntry().lock();
        if (weak_segment_process_list_entry)
        {
            PlanSegmentProcessList::EntryPtr segment_process_list_entry = std::move(weak_segment_process_list_entry);
            segment_id = segment_process_list_entry->getSegmentId();
            coordinator_address = segment_process_list_entry->getCoordinatorAddress();
            current_address = segment_process_list_entry->getCurrentAddress();
        }
        
        if (segment_id > 0)
            reportToCoordinator(coordinator_address, current_address, processor, query_id, std::chrono::system_clock::now(), segment_id);
    }   
}

void PipelineExecutor::cancel()
{
    cancelled = true;
    finish();

    std::lock_guard guard(processors_mutex);

    if (report_processors_profile)
        reportProcessorProfileOnCancel(processors);
    for (auto & processor : processors)
        processor->cancel();
}

void PipelineExecutor::finish()
{
    {
        std::lock_guard lock(task_queue_mutex);
        finished = true;
        async_task_queue.finish();
    }

    std::lock_guard guard(executor_contexts_mutex);

    for (auto & context : executor_contexts)
    {
        {
            std::lock_guard lock(context->mutex);
            context->wake_flag = true;
        }

        context->condvar.notify_one();
    }
}

void PipelineExecutor::execute(size_t num_threads)
{
    checkTimeLimit();
    try
    {
        executeImpl(num_threads);

        /// Execution can be stopped because of exception. Check and rethrow if any.
        for (auto & node : graph->nodes)
            if (node->exception)
                std::rethrow_exception(node->exception);

        /// Exception which happened in executing thread, but not at processor.
        for (auto & executor_context : executor_contexts)
            if (executor_context->exception)
                std::rethrow_exception(executor_context->exception);
    }
    catch (...)
    {
#ifndef NDEBUG
        LOG_TRACE(log, "Exception while executing query. Current state:\n{}", dumpPipeline());
#endif
        throw;
    }

    finalizeExecution();
}

bool PipelineExecutor::executeStep(std::atomic_bool * yield_flag)
{
    if (finished)
        return false;

    if (!is_execution_initialized)
        initializeExecution(1);

    executeStepImpl(0, 1, yield_flag);

    if (!finished)
        return true;

    /// Execution can be stopped because of exception. Check and rethrow if any.
    for (auto & node : graph->nodes)
        if (node->exception)
            std::rethrow_exception(node->exception);

    finalizeExecution();

    return false;
}

bool PipelineExecutor::checkTimeLimitSoft()
{
    if (process_list_element)
    {
        bool continuing = process_list_element->checkTimeLimitSoft();
        // We call cancel here so that all processors are notified and tasks waken up
        // so that the "break" is faster and doesn't wait for long events
        if (!continuing)
            cancel();
        return continuing;
    }

    return true;
}

bool PipelineExecutor::checkTimeLimit()
{
    bool continuing = checkTimeLimitSoft();
    if (!continuing)
        process_list_element->checkTimeLimit(); // Will throw if needed

    return continuing;
}

void PipelineExecutor::finalizeExecution()
{
    checkTimeLimit();

    if (cancelled)
        return;

    bool all_processors_finished = true;
    for (auto & node : graph->nodes)
    {
        if (node->status != ExecutingGraph::ExecStatus::Finished)
        {
            /// Single thread, do not hold mutex
            all_processors_finished = false;
            break;
        }
    }

    LOG_TRACE(log, "Pipeline: {}", dumpPipeline());

    if (process_list_element)
        process_list_element->dumpPipelineInfo(this);

    if (!all_processors_finished)
        throw Exception("Pipeline stuck. Current state:\n" + dumpPipeline(), ErrorCodes::LOGICAL_ERROR);
}

void PipelineExecutor::wakeUpExecutor(size_t thread_num)
{
    std::lock_guard guard(executor_contexts[thread_num]->mutex);
    executor_contexts[thread_num]->wake_flag = true;
    executor_contexts[thread_num]->condvar.notify_one();
}

void PipelineExecutor::executeSingleThread(size_t thread_num, size_t num_threads)
{
    executeStepImpl(thread_num, num_threads);

#ifndef NDEBUG
    auto & context = executor_contexts[thread_num];
    LOG_TRACE(log, "Thread finished. Total time: {} sec. Execution time: {} sec. Processing time: {} sec. Wait time: {} sec.", (context->total_time_ns / 1e9), (context->execution_time_ns / 1e9), (context->processing_time_ns / 1e9), (context->wait_time_ns / 1e9));
#endif
}

void PipelineExecutor::executeStepImpl(size_t thread_num, size_t num_threads, std::atomic_bool * yield_flag)
{
#ifndef NDEBUG
    Stopwatch total_time_watch;
#endif

    auto & context = executor_contexts[thread_num];
    auto & node = context->node;
    bool yield = false;

    while (!finished && !yield)
    {
        /// First, find any processor to execute.
        /// Just traverse graph and prepare any processor.
        while (!finished && node == nullptr)
        {
            {
                std::unique_lock lock(task_queue_mutex);

                if (!context->async_tasks.empty())
                {
                    node = context->async_tasks.front();
                    context->async_tasks.pop();
                    --num_waiting_async_tasks;

                    if (context->async_tasks.empty())
                        context->has_async_tasks = false;
                }
                else if (!task_queue.empty())
                    node = task_queue.pop(thread_num);

                if (node)
                {
                    if (!task_queue.empty() && !threads_queue.empty())
                    {
                        auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

                        if (threads_queue.has(thread_to_wake))
                            threads_queue.pop(thread_to_wake);
                        else
                            thread_to_wake = threads_queue.popAny();

                        lock.unlock();
                        wakeUpExecutor(thread_to_wake);
                    }

                    break;
                }

                if (threads_queue.size() + 1 == num_threads && async_task_queue.empty() && num_waiting_async_tasks == 0)
                {
                    lock.unlock();
                    finish();
                    break;
                }

#if defined(OS_LINUX)
                if (num_threads == 1)
                {
                    /// If we execute in single thread, wait for async tasks here.
                    auto res = async_task_queue.wait(lock);
                    if (!res)
                    {
                        /// The query had been cancelled (finished is also set)
                        if (finished)
                            break;
                        throw Exception("Empty task was returned from async task queue", ErrorCodes::LOGICAL_ERROR);
                    }

                    node = static_cast<ExecutingGraph::Node *>(res.data);
                    break;
                }
#endif

                threads_queue.push(thread_num);
            }

            {
                std::unique_lock lock(context->mutex);

                context->condvar.wait(lock, [&]
                {
                    return finished || context->wake_flag;
                });

                context->wake_flag = false;
            }
        }

        if (finished)
            break;

        while (node && !yield)
        {
            if (finished)
                break;

            addJob(node);

            {
                std::optional<Stopwatch> execution_time_watch;
#ifndef NDEBUG
                execution_time_watch.emplace();
#else
                if (need_processors_profiles)
                    execution_time_watch.emplace();
#endif
                node->job();

                if (need_processors_profiles) 
                {
                    node->processor->elapsed_us += execution_time_watch->elapsedMicroseconds();
                    node->processor->work_count++;
                }
#ifndef NDEBUG
                context->execution_time_ns += execution_time_watch->elapsed();
#endif          
            }

            if (node->exception)
                cancel();

            if (finished)
                break;

            if (!checkTimeLimitSoft())
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            {
                Queue queue;
                Queue async_queue;

                ++num_processing_executors;
                while (auto * task = expand_pipeline_task.load())
                    doExpandPipeline(task, true);

                /// Prepare processor after execution.
                {
                    auto lock = std::unique_lock<std::mutex>(node->status_mutex);
                    if (!prepareProcessor(node->processors_id, thread_num, queue, async_queue, std::move(lock)))
                        finish();
                }

                node = nullptr;

                /// Take local task from queue if has one.
                if (!queue.empty() && !context->has_async_tasks)
                {
                    node = queue.front();
                    queue.pop();
                }

                /// Push other tasks to global queue.
                if (!queue.empty() || !async_queue.empty())
                {
                    std::unique_lock lock(task_queue_mutex);

#if defined(OS_LINUX)
                    while (!async_queue.empty() && !finished)
                    {
                        async_task_queue.addTask(thread_num, async_queue.front(), async_queue.front()->processor->schedule());
                        async_queue.pop();
                    }
#endif

                    while (!queue.empty() && !finished)
                    {
                        task_queue.push(queue.front(), thread_num);
                        queue.pop();
                    }

                    if (!threads_queue.empty() && !task_queue.empty() && !finished)
                    {
                        auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

                        if (threads_queue.has(thread_to_wake))
                            threads_queue.pop(thread_to_wake);
                        else
                            thread_to_wake = threads_queue.popAny();

                        lock.unlock();

                        wakeUpExecutor(thread_to_wake);
                    }
                }

                --num_processing_executors;
                while (auto * task = expand_pipeline_task.load())
                    doExpandPipeline(task, false);
            }

#ifndef NDEBUG
            context->processing_time_ns += processing_time_watch.elapsed();
#endif

            /// We have executed single processor. Check if we need to yield execution.
            if (yield_flag && *yield_flag)
                yield = true;
        }
    }

#ifndef NDEBUG
    context->total_time_ns += total_time_watch.elapsed();
    context->wait_time_ns = context->total_time_ns - context->execution_time_ns - context->processing_time_ns;
#endif
}

void PipelineExecutor::initializeExecution(size_t num_threads)
{
    is_execution_initialized = true;

    threads_queue.init(num_threads);
    task_queue.init(num_threads);

    {
        std::lock_guard guard(executor_contexts_mutex);

        executor_contexts.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            executor_contexts.emplace_back(std::make_unique<ExecutorContext>());
    }

    Stack stack;
    addChildlessProcessorsToStack(stack);

    {
        std::lock_guard lock(task_queue_mutex);

        Queue queue;
        Queue async_queue;
        size_t next_thread = 0;

        while (!stack.empty())
        {
            UInt64 proc = stack.top();
            stack.pop();

            prepareProcessor(proc, 0, queue, async_queue, std::unique_lock<std::mutex>(graph->nodes[proc]->status_mutex));

            while (!queue.empty())
            {
                task_queue.push(queue.front(), next_thread);
                queue.pop();

                ++next_thread;
                if (next_thread >= num_threads)
                    next_thread = 0;
            }

            while (!async_queue.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Async is only possible after work() call. Processor {}",
                                async_queue.front()->processor->getName());
        }
    }
}

void PipelineExecutor::executeImpl(size_t num_threads)
{
    OpenTelemetrySpanHolder span("PipelineExecutor::executeImpl()");

    initializeExecution(num_threads);

    using ThreadsData = std::vector<ThreadFromGlobalPool>;
    ThreadsData threads;
    threads.reserve(num_threads);

    bool finished_flag = false;

    SCOPE_EXIT_SAFE(
        if (!finished_flag)
        {
            finish();

            for (auto & thread : threads)
                if (thread.joinable())
                    thread.join();
        }
    );

    if (num_threads > 1)
    {
        auto thread_group = CurrentThread::getGroup();

        for (size_t i = 0; i < num_threads; ++i)
        {
            threads.emplace_back([this, thread_group, thread_num = i, num_threads]
            {
                /// ThreadStatus thread_status;

                setThreadName("QueryPipelineEx");

                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                );

                try
                {
                    executeSingleThread(thread_num, num_threads);
                }
                catch (...)
                {
                    /// In case of exception from executor itself, stop other threads.
                    finish();
                    executor_contexts[thread_num]->exception = std::current_exception();
                }
            });
        }

#if defined(OS_LINUX)
        {
            /// Wait for async tasks.
            std::unique_lock lock(task_queue_mutex);
            while (auto task = async_task_queue.wait(lock))
            {
                auto * node = static_cast<ExecutingGraph::Node *>(task.data);
                executor_contexts[task.thread_num]->async_tasks.push(node);
                executor_contexts[task.thread_num]->has_async_tasks = true;
                ++num_waiting_async_tasks;

                if (threads_queue.has(task.thread_num))
                {
                    threads_queue.pop(task.thread_num);
                    wakeUpExecutor(task.thread_num);
                }
            }
        }
#endif

        for (auto & thread : threads)
            if (thread.joinable())
                thread.join();
    }
    else
        executeSingleThread(0, num_threads);

    finished_flag = true;
}

String PipelineExecutor::dumpPipeline() const
{
    for (const auto & node : graph->nodes)
    {
        {
            WriteBufferFromOwnString buffer;
            buffer << "(" << node->num_executed_jobs << " jobs";

#ifndef NDEBUG
            buffer << ", execution time: " << node->execution_time_ns / 1e9 << " sec.";
            buffer << ", preparation time: " << node->preparation_time_ns / 1e9 << " sec.";
#endif

            buffer << ")";
            node->processor->setDescription(buffer.str());
        }
    }

    std::vector<IProcessor::Status> statuses;
    std::vector<IProcessor *> proc_list;
    statuses.reserve(graph->nodes.size());
    proc_list.reserve(graph->nodes.size());

    for (const auto & node : graph->nodes)
    {
        proc_list.emplace_back(node->processor);
        statuses.emplace_back(node->last_processor_status);
    }

    WriteBufferFromOwnString out;
    printPipeline(processors, statuses, out);
    out.finalize();

    return out.str();
}

void PipelineExecutor::dumpPipelineToFile(const String & suffix) const
{
    if (process_list_element)
    {
        if (process_list_element->getContext())
        {
            String query_id = process_list_element->getContext()->getCurrentQueryId();
            String pipeline_log_path = process_list_element->getContext()->getPipelineLogpath();
            String file_path = pipeline_log_path + "/" + query_id + "_" + suffix + ".dot";

            auto pipeline_string = dumpPipeline();
            WriteBufferFromFile buf(file_path);
            buf.write(pipeline_string.c_str(), pipeline_string.size());
            buf.close();
        }
    }
}

}
