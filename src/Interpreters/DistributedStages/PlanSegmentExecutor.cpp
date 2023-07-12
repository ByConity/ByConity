/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <exception>
#include <memory>
#include <vector>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/Brpc/AsyncRegisterResult.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/Exchange/LoadBalancedExchangeSink.h>
#include <Processors/Exchange/MultiPartitionExchangeSink.h>
#include <Processors/Exchange/RepartitionTransform.h>
#include <Processors/Exchange/SinglePartitionExchangeSink.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Transforms/BufferedCopyTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/ResizeProcessor.h>
#include <Protos/plan_segment_manager.pb.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/QueryPlan.h>
#include <fmt/core.h>
#include <Common/Brpc/BrpcChannelPoolOptions.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace ProfileEvents
{
    extern const Event SystemTimeMicroseconds;
    extern const Event UserTimeMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

void PlanSegmentExecutor::prepareSegmentInfo() const
{
    query_log_element->client_info = plan_segment->getContext()->getClientInfo();
    query_log_element->segment_id = plan_segment->getPlanSegmentId();
    query_log_element->segment_parallel = plan_segment->getParallelSize();
    query_log_element->segment_parallel_index = plan_segment->getParallelIndex();
    query_log_element->type = QueryLogElementType::QUERY_START;
    query_log_element->event_time = time(nullptr);
    query_log_element->query_start_time = time(nullptr);
}

PlanSegmentExecutor::PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_)
    : context(std::move(context_))
    , plan_segment(std::move(plan_segment_))
    , plan_segment_outputs(plan_segment->getPlanSegmentOutputs())
    , logger(&Poco::Logger::get("PlanSegmentExecutor"))
    , query_log_element(std::make_unique<QueryLogElement>())
{
    options = ExchangeUtils::getExchangeOptions(context);
    prepareSegmentInfo();
}

PlanSegmentExecutor::PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_, ExchangeOptions options_)
    : context(std::move(context_))
    , plan_segment(std::move(plan_segment_))
    , plan_segment_outputs(plan_segment->getPlanSegmentOutputs())
    , options(std::move(options_))
    , logger(&Poco::Logger::get("PlanSegmentExecutor"))
    , query_log_element(std::make_unique<QueryLogElement>())
{
    prepareSegmentInfo();
}

PlanSegmentExecutor::~PlanSegmentExecutor() noexcept
{
    try
    {
        if (context->getSettingsRef().log_queries && query_log_element->type >= context->getSettingsRef().log_queries_min_type)
        {
            if (auto query_log = context->getQueryLog())
                query_log->add(*query_log_element);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            logger,
            "QueryLogElement:[query_id-{}, segment_id-{}, segment_parallel_index-{}] save to table fail with exception:{}",
            query_log_element->client_info.initial_query_id,
            query_log_element->segment_id,
            query_log_element->segment_parallel_index,
            getCurrentExceptionCode());
    }
}

RuntimeSegmentsStatus PlanSegmentExecutor::execute(ThreadGroupStatusPtr thread_group)
{
    LOG_DEBUG(logger, "execute PlanSegment:\n" + plan_segment->toString());
    try
    {
        doExecute(std::move(thread_group));

        runtime_segment_status.query_id = plan_segment->getQueryId();
        runtime_segment_status.segment_id = plan_segment->getPlanSegmentId();
        runtime_segment_status.is_succeed = true;
        runtime_segment_status.is_canceled = false;
        runtime_segment_status.code = 0;
        runtime_segment_status.message = "execute success";

        query_log_element->type = QueryLogElementType::QUERY_FINISH;
        sendSegmentStatus(runtime_segment_status);
        return runtime_segment_status;
    }
    catch (...)
    {
        int exception_code = getCurrentExceptionCode();
        auto exception_message = getCurrentExceptionMessage(false);

        query_log_element->type = QueryLogElementType::EXCEPTION_WHILE_PROCESSING;
        query_log_element->exception_code = exception_code;
        query_log_element->stack_trace = exception_message;

        const auto & host = extractExchangeStatusHostPort(plan_segment->getCurrentAddress());
        runtime_segment_status.query_id = plan_segment->getQueryId();
        runtime_segment_status.segment_id = plan_segment->getPlanSegmentId();
        runtime_segment_status.is_succeed = false;
        runtime_segment_status.is_canceled = false;
        runtime_segment_status.code = exception_code;
        runtime_segment_status.message = "Worker host:" + host + ", exception:" + exception_message;
        if (exception_code == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        {
            // ErrorCodes::MEMORY_LIMIT_EXCEEDED don't print stack trace.
            LOG_ERROR(
                logger,
                " [{}_{}] Query has excpetion with code: {}, msg: {}",
                plan_segment->getQueryId(),
                plan_segment->getPlanSegmentId(),
                exception_code,
                exception_message);
        }
        else
        {
            tryLogCurrentException(
                logger,
                fmt::format(
                    "[{}_{}]: Query has excpetion with code: {}, detail \n",
                    plan_segment->getQueryId(),
                    plan_segment->getPlanSegmentId(),
                    exception_code));
        }
        if (exception_code == ErrorCodes::QUERY_WAS_CANCELLED)
            runtime_segment_status.is_canceled = true;
        sendSegmentStatus(runtime_segment_status);
        return runtime_segment_status;
    }

    //TODO notify segment scheduler with finished or exception status.
}

BlockIO PlanSegmentExecutor::lazyExecute(bool /*add_output_processors*/)
{
    BlockIO res;
    // Will run as master query and already initialized
    if (!CurrentThread::get().getQueryContext() || CurrentThread::get().getQueryContext().get() != context.get())
        throw Exception("context not match", ErrorCodes::LOGICAL_ERROR);

    res.plan_segment_process_entry = context->getPlanSegmentProcessList().insert(*plan_segment, context);

    res.pipeline = std::move(*buildPipeline());
    context->setPlanSegmentProcessListEntry(res.plan_segment_process_entry);
    return res;
}

void PlanSegmentExecutor::collectSegmentQueryRuntimeMetric(const QueryStatus * query_status)
{
    auto query_status_info = query_status->getInfo(true, context->getSettingsRef().log_profile_events);
    const auto & query_access_info = context->getQueryAccessInfo();

    query_log_element->read_bytes = query_status_info.read_bytes;
    query_log_element->read_rows = query_status_info.read_rows;
    query_log_element->written_bytes = query_status_info.written_bytes;
    query_log_element->written_rows = query_status_info.written_bytes;
    query_log_element->memory_usage = query_status_info.peak_memory_usage > 0 ? query_status_info.peak_memory_usage : 0;
    query_log_element->query_duration_ms = query_status_info.elapsed_seconds * 1000;
    query_log_element->max_io_time_thread_ms = query_status_info.max_io_time_thread_ms;
    query_log_element->max_io_time_thread_name = query_status_info.max_io_time_thread_name;
    query_log_element->thread_ids = std::move(query_status_info.thread_ids);
    query_log_element->profile_counters = query_status_info.profile_counters;
    query_log_element->max_thread_io_profile_counters = query_status_info.max_io_thread_profile_counters;

    query_log_element->query_tables = query_access_info.tables;
}

void PlanSegmentExecutor::doExecute(ThreadGroupStatusPtr thread_group)
{
    std::optional<CurrentThread::QueryScope> query_scope;

    if (!thread_group)
    {
        if (!CurrentThread::getGroup())
        {
            query_scope.emplace(context); // Running as master query and not initialized
        }
        else
        {
            // Running as master query and already initialized
            if (!CurrentThread::get().getQueryContext() || CurrentThread::get().getQueryContext().get() != context.get())
                throw Exception("context not match", ErrorCodes::LOGICAL_ERROR);
        }
    }
    else
    {
        // Running as slave query in a thread different from master query
        if (CurrentThread::getGroup())
            throw Exception("There is a query attacted to context", ErrorCodes::LOGICAL_ERROR);

        if (CurrentThread::getQueryId() != plan_segment->getQueryId())
            throw Exception("Not the same distributed query", ErrorCodes::LOGICAL_ERROR);

        CurrentThread::attachTo(thread_group);
    }

    PlanSegmentProcessList::EntryPtr process_plan_segment_entry = context->getPlanSegmentProcessList().insert(*plan_segment, context);
    context->setPlanSegmentProcessListEntry(process_plan_segment_entry);

    QueryPipelinePtr pipeline;
    BroadcastSenderPtrs senders;
    buildPipeline(pipeline, senders);

    QueryStatus * query_status = &process_plan_segment_entry->get();
    context->setProcessListElement(query_status);
    pipeline->setProcessListElement(query_status);

    auto pipeline_executor = pipeline->execute();

    size_t max_threads = context->getSettingsRef().max_threads;
    if (max_threads)
        pipeline->setMaxThreads(max_threads);
    size_t num_threads = pipeline->getNumThreads();
    LOG_DEBUG(
        logger,
        "Runing plansegment id {}, segment: {} pipeline with {} threads",
        plan_segment->getQueryId(),
        plan_segment->getPlanSegmentId(),
        num_threads);

    pipeline_executor->execute(num_threads);

    if (CurrentThread::getGroup())
    {
        runtime_segment_status.metrics.cpu_micros = CurrentThread::getGroup()->performance_counters[ProfileEvents::SystemTimeMicroseconds]
                + CurrentThread::getGroup()->performance_counters[ProfileEvents::UserTimeMicroseconds];
    }
    for (const auto & sender : senders)
        sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "Upstream pipeline finished");

    if (context->getSettingsRef().log_queries)
        collectSegmentQueryRuntimeMetric(query_status);
}

QueryPipelinePtr PlanSegmentExecutor::buildPipeline()
{
    QueryPipelinePtr pipeline = plan_segment->getQueryPlan().buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromPlanSegment(plan_segment.get(), context));
    registerAllExchangeReceivers(*pipeline, context->getSettingsRef().exchange_wait_accept_max_timeout_ms);
    return pipeline;
}

void PlanSegmentExecutor::buildPipeline(QueryPipelinePtr & pipeline, BroadcastSenderPtrs & senders)
{
    String coordinator_address = extractExchangeStatusHostPort(plan_segment->getCoordinatorAddress());

    std::vector<BroadcastSenderPtrs> senders_list;
    for (const auto &cur_plan_segment_output : plan_segment_outputs)
    {
        size_t exchange_parallel_size = cur_plan_segment_output->getExchangeParallelSize();
        size_t parallel_size = cur_plan_segment_output->getParallelSize();
        ExchangeMode exchange_mode = cur_plan_segment_output->getExchangeMode();
        size_t exchange_id = cur_plan_segment_output->getExchangeId();
        const Block & header = cur_plan_segment_output->getHeader();
        BroadcastSenderPtrs current_exchange_senders;

        if (exchange_mode == ExchangeMode::BROADCAST)
            exchange_parallel_size = 1;

        /// output partitions num = num of plan_segment * exchange size
        /// for example, if downstream plansegment size is 2 (parallel_id is 0 and 1) and exchange_parallel_size is 4
        /// Exchange Sink will repartition data into 8 partition(2*4), partition id is range from 1 to 8.
        /// downstream plansegment and consumed partitions table:
        /// plansegment parallel_id :  partition id
        /// -----------------------------------------------
        /// 0                       : 1,2,3,4
        /// 1                       : 5,6,7,8
        size_t total_partition_num = exchange_parallel_size == 0 ? parallel_size : parallel_size * exchange_parallel_size;

        if (total_partition_num == 0)
            throw Exception("Total partition number should not be zero", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0; i < total_partition_num; i++)
        {
            size_t partition_id = i + 1;
            auto data_key = std::make_shared<ExchangeDataKey>(
                plan_segment->getQueryId(), exchange_id, partition_id, coordinator_address);
            BroadcastSenderProxyPtr sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
            sender->accept(context, header);
            current_exchange_senders.emplace_back(std::move(sender));
        }

        senders_list.emplace_back(std::move(current_exchange_senders));
    }

    pipeline = plan_segment->getQueryPlan().buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromPlanSegment(plan_segment.get(), context)
    );

    registerAllExchangeReceivers(*pipeline, context->getSettingsRef().exchange_wait_accept_max_timeout_ms);

    pipeline->setMaxThreads(pipeline->getNumThreads());

    if (plan_segment->getPlanSegmentOutputs().empty())
        throw Exception("PlanSegment has no output", ErrorCodes::LOGICAL_ERROR);

    size_t sink_num = 0;
    for (size_t i = 0; i < plan_segment_outputs.size(); ++i)
    {
        const auto &cur_plan_segment_output = plan_segment_outputs[i];
        const auto &current_exchange_senders = senders_list[i];
        ExchangeMode exchange_mode = cur_plan_segment_output->getExchangeMode();
        bool keep_order = cur_plan_segment_output->needKeepOrder() || context->getSettingsRef().exchange_enable_force_keep_order;

        auto output_size = context->getSettingsRef().exchange_unordered_output_parallel_size;

        switch (exchange_mode)
        {
            case ExchangeMode::REPARTITION:
            case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            case ExchangeMode::GATHER:
            {
                size_t output_num = pipeline->getNumStreams();
                size_t partition_num = current_exchange_senders.size();
                bool need_resize =
                    keep_order 
                    && context->getSettingsRef().exchange_enable_keep_order_parallel_shuffle
                    && partition_num > 1;
                sink_num += (need_resize) ? output_num*partition_num : output_size;
                break;
            }
            case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
            case ExchangeMode::BROADCAST:
                sink_num += output_size;
                break;
            default:
                throw Exception(
                    "Cannot find expected ExchangeMode " + std::to_string(UInt8(exchange_mode)),
                    ErrorCodes::LOGICAL_ERROR
                );
        }
    }

    pipeline->transform(
        [&](OutputPortRawPtrs ports) -> Processors {
            Processors new_processors;
            std::vector<OutputPortRawPtrs> segs_output_ports(plan_segment_outputs.size(), OutputPortRawPtrs());

            /*
             * 1. initial pipeline state
             *
             *  _____     /------ ports[0]
             * |  T  | ---|------ ports[1]
             * |_____|    \------ ports[2]
             */

            if (plan_segment_outputs.size() > 1)
            {
                /*
                 * 2.1. If plan segment has multi outputs, add copyTransfrom to pipeline.
                 *
                 *                                     _______________
                 *            /------ ports[0] -------| CopyTransform1| ------------- ports[0] for plan_segment_outputs[0]
                 *            |                       |_______________|        \----- ports[1] for plan_segment_outputs[1]
                 *  _____     |                        _______________
                 * |  T  | ---|------ ports[1] -------| CopyTransform2| ------------- ports[0] for plan_segment_outputs[0]
                 * |_____|    |                       |_______________|        \----- ports[1] for plan_segment_outputs[1]
                 *            |                        _______________
                 *            \------ ports[2] -------| CopyTransform3| ------------- ports[0] for plan_segment_outputs[0]
                 *                                    |_______________|        \----- ports[1] for plan_segment_outputs[1]
                 * 
                 * Save the ports for plan_segment_outputs[0] in segs_output_ports[0],
                 * Save the ports for plan_segment_outputs[1] in segs_output_ports[1]...
                 */
                for (const auto & port : ports)
                {
                    const auto & header = port->getHeader();
                    auto copy_transform = std::make_shared<CopyTransform>(header, plan_segment_outputs.size());
                    auto &copy_outputs = copy_transform->getOutputs();

                    connect(*port, copy_transform->getInputs().front());

                    size_t seg_id = 0;
                    for (auto & copy_output : copy_outputs)
                    {
                        segs_output_ports[seg_id].push_back(&copy_output);
                        ++seg_id;
                    }

                    new_processors.emplace_back(std::move(copy_transform));
                }
            }
            else
            {
                /*
                 * 2.2. If there is only on plan_segment_output, than there is no need to add copyTransform.
                 */
                segs_output_ports[0] = ports;
            }

            for (size_t i = 0; i < segs_output_ports.size(); ++i)
            {
                auto &cur_plan_segment_output = plan_segment_outputs[i];
                auto &current_exchange_senders = senders_list[i];
                ExchangeMode exchange_mode = cur_plan_segment_output->getExchangeMode();
                bool keep_order = cur_plan_segment_output->needKeepOrder() || context->getSettingsRef().exchange_enable_force_keep_order;
                const auto & header = segs_output_ports[i][0]->getHeader();

                auto output_size = context->getSettingsRef().exchange_unordered_output_parallel_size;
                if (!keep_order && output_size)
                {
                    /*
                     * 3.1. Add ResizeProcessor to pipeline.
                     *  _______________
                     * | CopyTransform1| ------------- ports[0] for plan_segment_outputs[0] -------------------------\
                     * |_______________|        \----- ports[1] for plan_segment_outputs[1]->ResizeProcessor 2       |
                     *  _______________                                                                              |
                     * | CopyTransform2| ------------- ports[0] for plan_segment_outputs[0] -------------------------|-------- ResizeProcessor 1, for seg 1
                     * |_______________|        \----- ports[1] for plan_segment_outputs[1]->ResizeProcessor 2       |         output ports num is output_size
                     *  _______________                                                                              |
                     * | CopyTransform3| ------------- ports[0] for plan_segment_outputs[0] -------------------------/
                     * |_______________|        \----- ports[1] for plan_segment_outputs[1]->ResizeProcessor 2
                     * 
                     * If there is no CopyTransform, than pipeline will be:
                     *  _____     /------ ports[0] ------\
                     * |  T  | ---|------ ports[1] ------|-------ResizeProcessor 1, for seg 1
                     * |_____|    \------ ports[2] ------/       output ports num is output_size
                     */
                    auto resize = std::make_shared<ResizeProcessor>(header, segs_output_ports[i].size(), output_size);
                    auto &resize_inputs = resize->getInputs();
                    auto &resize_outputs = resize->getOutputs();

                    size_t input_index = 0;
                    for (auto & input : resize_inputs)
                    {
                        connect(*segs_output_ports[i][input_index], input);
                        ++input_index;
                    }

                    segs_output_ports[i].clear();
                    for (auto & output : resize_outputs)
                    {
                        segs_output_ports[i].emplace_back(&output);
                    }

                    new_processors.emplace_back(std::move(resize));
                }
                // else 3.2. No need to add ResizeProcessor.

                /* 4. Add ExchangeSink to pipeline. */
                Processors current_new_processors;

                switch (exchange_mode)
                {
                    case ExchangeMode::REPARTITION:
                    case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
                    case ExchangeMode::GATHER:
                        current_new_processors = buildRepartitionExchangeSink(
                            current_exchange_senders, keep_order, i, header, segs_output_ports[i]);
                        break;
                    case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
                        current_new_processors = buildLoadBalancedExchangeSink(
                            current_exchange_senders, i, header, segs_output_ports[i]);
                        break;
                    case ExchangeMode::BROADCAST:
                        current_new_processors = buildBroadcastExchangeSink(
                            current_exchange_senders, i, header, segs_output_ports[i]);
                        break;
                    default:
                        throw Exception(
                            "Cannot find expected ExchangeMode " + std::to_string(UInt8(exchange_mode)),
                            ErrorCodes::LOGICAL_ERROR
                        );
                }

                new_processors.insert(new_processors.end(), current_new_processors.begin(), current_new_processors.end());
            }

            return new_processors;
        },
        sink_num
    );

    for (size_t i = 0; i < plan_segment_outputs.size(); ++i)
    {
        auto &current_exchange_senders = senders_list[i];
        for (auto &sender:current_exchange_senders)
        {
            senders.emplace_back(std::move(sender));
        }
    }

    if (senders.empty())
        throw Exception("Plan segment has no exchange sender!", ErrorCodes::LOGICAL_ERROR);
}

void PlanSegmentExecutor::registerAllExchangeReceivers(const QueryPipeline & pipeline, UInt32 register_timeout_ms)
{
    const Processors & procesors = pipeline.getProcessors();
    std::vector<AsyncRegisterResult> async_results;
    std::vector<LocalBroadcastChannel *> local_receivers;
    std::exception_ptr exception;

    try
    {
        for (const auto & processor : procesors)
        {
            auto exchange_source_ptr = std::dynamic_pointer_cast<ExchangeSource>(processor);
            if (!exchange_source_ptr)
                continue;
            auto * receiver_ptr = exchange_source_ptr->getReceiver().get();
            auto * local_receiver = dynamic_cast<LocalBroadcastChannel *>(receiver_ptr);
            if (local_receiver)
                local_receivers.push_back(local_receiver);
            else
            {
                auto * brpc_receiver = dynamic_cast<BrpcRemoteBroadcastReceiver *>(receiver_ptr);
                if (unlikely(!brpc_receiver))
                    throw Exception("Unexpected SubReceiver Type: " + std::string(typeid(receiver_ptr).name()), ErrorCodes::LOGICAL_ERROR);
                async_results.emplace_back(brpc_receiver->registerToSendersAsync(register_timeout_ms));
            }
        }

        for (auto * local_receiver : local_receivers)
            local_receiver->registerToSenders(register_timeout_ms);
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    /// Wait all brpc register rpc done
    for (auto & res : async_results)
        brpc::Join(res.cntl->call_id());

    if (exception)
        std::rethrow_exception(std::move(exception));

    /// get result
    for (auto & res : async_results)
    {
        // if exchange_enable_force_remote_mode = 1, sender and receiver in same process and sender stream may close before rpc end
        if (res.cntl->ErrorCode() == brpc::EREQUEST && boost::algorithm::ends_with(res.cntl->ErrorText(), "was closed before responded"))
        {
            LOG_INFO(
                &Poco::Logger::get("PlanSegmentExecutor"),
                "Receiver register sender successfully but sender already finished, host-{} , data_key: {}_{}_{}_{}",
                butil::endpoint2str(res.cntl->remote_side()).c_str(),
                res.request->query_id(),
                res.request->exchange_id(),
                res.request->parallel_id(),
                res.request->coordinator_address());
            continue;
        }
        res.channel->assertController(*res.cntl);
        LOG_TRACE(
            &Poco::Logger::get("PlanSegmentExecutor"),
            "Receiver register sender successfully, host-{} , data_key: {}_{}_{}_{}",
            butil::endpoint2str(res.cntl->remote_side()).c_str(),
            res.request->query_id(),
            res.request->exchange_id(),
            res.request->parallel_id(),
            res.request->coordinator_address());
    }
}

Processors PlanSegmentExecutor::buildRepartitionExchangeSink(
    BroadcastSenderPtrs & senders, bool keep_order, size_t output_index, const Block &header, OutputPortRawPtrs &ports)
{
    Processors new_processors;

    ColumnsWithTypeAndName arguments;
    ColumnNumbers argument_numbers;
    for (const auto & column_name : plan_segment->getPlanSegmentOutput()->getShufflekeys())
    {
        arguments.emplace_back(plan_segment_outputs[output_index]->getHeader().getByName(column_name));
        argument_numbers.emplace_back(plan_segment_outputs[output_index]->getHeader().getPositionByName(column_name));
    }
    auto repartition_func = RepartitionTransform::getDefaultRepartitionFunction(arguments, context);
    size_t partition_num = senders.size();

    if (keep_order && context->getSettingsRef().exchange_enable_keep_order_parallel_shuffle && partition_num > 1)
    {
        size_t output_num = ports.size();
        size_t sink_num = output_num * partition_num;

        new_processors.resize(1+output_num+sink_num);

        for (const auto & port : ports)
        {
            /* create one repartition transform for per port. */
            auto repartition_transform = std::make_shared<RepartitionTransform>(header, partition_num, argument_numbers, repartition_func);
            auto &repartition_outputs = repartition_transform->getOutputs();
            connect(*port, repartition_transform->getInputs().front());

            for (auto & repartition_output : repartition_outputs)
            {
                /* create BufferedCopyTransform, and connect RepartitionTransform(output port i) to BufferedCopyTransform */
                auto copy_transform = std::make_shared<BufferedCopyTransform>(header, partition_num, 20);
                connect(repartition_output, copy_transform->getInputPort());

                /* create SinglePartitionExchangeSink, and connect BufferedCopyTransform to SinglePartitionExchangeSink */
                auto & copy_outputs = copy_transform->getOutputs();
                size_t partition_id = 0;
                for (auto & copy_output : copy_outputs)
                {
                    String name = SinglePartitionExchangeSink::generateName(plan_segment_outputs[output_index]->getExchangeId());
                    auto exchange_sink =
                        std::make_shared<SinglePartitionExchangeSink>(header, senders[partition_id], partition_id, options, name);
                    connect(copy_output, exchange_sink->getPort());
                    new_processors.emplace_back(std::move(exchange_sink));

                    ++partition_id;
                }

                new_processors.emplace_back(std::move(copy_transform));
            }

            new_processors.emplace_back(repartition_transform);
        }
    }
    else
    {
        for (const auto & port : ports)
        {
            String name = MultiPartitionExchangeSink::generateName(plan_segment_outputs[output_index]->getExchangeId());
            auto exchange_sink =
                std::make_shared<MultiPartitionExchangeSink>(header, senders, repartition_func, argument_numbers, options, name);
            connect(*port, exchange_sink->getInputs().front());

            new_processors.emplace_back(std::move(exchange_sink));
        }
    }

    return new_processors;
}

Processors PlanSegmentExecutor::buildBroadcastExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports)
{
    /// For broadcast exchange, we all 1:1 remote sender to one 1:N remote sender and can avoid duplicated serialization
    ExchangeUtils::mergeSenders(senders);
    LOG_DEBUG(logger, "After merge, broadcast sink size {}", senders.size());
    Processors new_processors;

    for (auto &port : ports)
    {
        String name = BroadcastExchangeSink::generateName(plan_segment_outputs[output_index]->getExchangeId());
        auto exchange_sink =
            std::make_shared<BroadcastExchangeSink>(header, senders, options, name);
        connect(*port, exchange_sink->getInputs().front());
        
        new_processors.emplace_back(std::move(exchange_sink));
    }

    return new_processors;
}

Processors PlanSegmentExecutor::buildLoadBalancedExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports)
{
    Processors new_processors;

    for (auto &port : ports)
    {
        String name = LoadBalancedExchangeSink::generateName(plan_segment_outputs[output_index]->getExchangeId());
        auto exchange_sink =
            std::make_shared<LoadBalancedExchangeSink>(header, senders, name);
        connect(*port, exchange_sink->getInputs().front());

        new_processors.emplace_back(std::move(exchange_sink));
    }

    return new_processors;
}

void PlanSegmentExecutor::sendSegmentStatus(const RuntimeSegmentsStatus & status) noexcept
{
    try
    {
        if (!options.need_send_plan_segment_status)
            return;
        auto address = extractExchangeStatusHostPort(plan_segment->getCoordinatorAddress());

        std::shared_ptr<RpcClient> rpc_client
            = RpcChannelPool::getInstance().getClient(address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
        Protos::PlanSegmentManagerService_Stub manager(&rpc_client->getChannel());
        brpc::Controller cntl;
        Protos::SendPlanSegmentStatusRequest request;
        Protos::SendPlanSegmentStatusResponse response;
        request.set_query_id(status.query_id);
        request.set_segment_id(status.segment_id);
        request.set_is_succeed(status.is_succeed);
        request.set_is_canceled(status.is_canceled);
        status.metrics.setProtos(*request.mutable_metrics());
        request.set_code(status.code);
        request.set_message(status.message);

        manager.sendPlanSegmentStatus(&cntl, &request, &response, nullptr);
        rpc_client->assertController(cntl);
        LOG_TRACE(logger, "PlanSegment-{} send status to coordinator successfully, query id-{} status.cpu_micros-{}.", request.segment_id(), request.query_id(), status.metrics.cpu_micros);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}
}
