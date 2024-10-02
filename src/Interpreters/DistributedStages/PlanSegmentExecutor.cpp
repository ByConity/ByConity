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

#include <cstddef>
#include <exception>
#include <memory>
#include <vector>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Interpreters/DistributedStages/PlanSegmentReport.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorProfile.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Interpreters/executeQueryHelper.h>
#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/Batch/Writer/DiskPartitionWriter.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/Brpc/AsyncRegisterResult.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/DataTrans/MultiPathReceiver.h>
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
#include <Processors/Executors/ExecutingGraph.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/BufferedCopyTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Protos/plan_segment_manager.pb.h>
#include <Protos/registry.pb.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/QueryPlan.h>
#include <brpc/callback.h>
#include <fmt/core.h>
#include <incubator-brpc/src/brpc/controller.h>
#include <Poco/Logger.h>
#include <Common/Brpc/BrpcChannelPoolOptions.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/time.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/scope_guard_safe.h>
#include <common/types.h>
#include "Interpreters/sendPlanSegment.h"

namespace ProfileEvents
{
    extern const Event SystemTimeMicroseconds;
    extern const Event UserTimeMicroseconds;
    extern const Event PlanSegmentInstanceRetry;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
    extern const int BSP_CLEANUP_PREVIOUS_SEGMENT_INSTANCE_FAILED;
    extern const int BSP_WRITE_DATA_FAILED;
}

void PlanSegmentExecutor::prepareSegmentInfo() const
{
    query_log_element->client_info = context->getClientInfo();
    query_log_element->segment_id = plan_segment->getPlanSegmentId();
    query_log_element->segment_parallel = plan_segment->getParallelSize();
    query_log_element->segment_parallel_index = plan_segment_instance->info.parallel_id;
    query_log_element->type = QueryLogElementType::QUERY_START;
    const auto current_time = std::chrono::system_clock::now();
    query_log_element->event_time = time_in_seconds(current_time);
    query_log_element->event_time_microseconds = time_in_microseconds(current_time);
    query_log_element->query_start_time = time_in_seconds(current_time);
    query_log_element->query_start_time_microseconds = time_in_microseconds(current_time);
}

PlanSegmentExecutor::PlanSegmentExecutor(
    PlanSegmentInstancePtr plan_segment_instance_, ContextMutablePtr context_, PlanSegmentProcessList::EntryPtr process_plan_segment_entry_)
    : process_plan_segment_entry(std::move(process_plan_segment_entry_))
    , context(std::move(context_))
    , plan_segment_instance(std::move(plan_segment_instance_))
    , plan_segment(plan_segment_instance->plan_segment.get())
    , plan_segment_outputs(plan_segment_instance->plan_segment->getPlanSegmentOutputs())
    , logger(&Poco::Logger::get("PlanSegmentExecutor"))
    , query_log_element(std::make_unique<QueryLogElement>())
{
    options = ExchangeUtils::getExchangeOptions(context);
    prepareSegmentInfo();
}

PlanSegmentExecutor::PlanSegmentExecutor(
    PlanSegmentInstancePtr plan_segment_instance_,
    ContextMutablePtr context_,
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry_,
    ExchangeOptions options_)
    : process_plan_segment_entry(std::move(process_plan_segment_entry_))
    , context(std::move(context_))
    , plan_segment_instance(std::move(plan_segment_instance_))
    , plan_segment(plan_segment_instance->plan_segment.get())
    , plan_segment_outputs(plan_segment_instance->plan_segment->getPlanSegmentOutputs())
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

    for (const auto & id : plan_segment->getRuntimeFilters())
        RuntimeFilterManager::getInstance().removeDynamicValue(plan_segment->getQueryId(), id);
}

std::optional<PlanSegmentExecutor::ExecutionResult> PlanSegmentExecutor::execute()
{
    LOG_DEBUG(logger, "execute PlanSegment:\n" + plan_segment->toString());
    try
    {
        context->initPlanSegmentExHandler();
        doExecute();

        query_log_element->type = QueryLogElementType::QUERY_FINISH;
        const auto finish_time = std::chrono::system_clock::now();
        query_log_element->event_time = time_in_seconds(finish_time);
        query_log_element->event_time_microseconds = time_in_microseconds(finish_time);

        return convertSuccessPlanSegmentStatusToResult(
            context, plan_segment_instance->info, final_progress, sender_metrics, plan_segment_outputs, segment_profile);
    }
    catch (...)
    {
        int exception_code = getCurrentExceptionCode();
        auto exception_message = getCurrentExceptionMessage(false);

        query_log_element->type = QueryLogElementType::EXCEPTION_WHILE_PROCESSING;
        query_log_element->exception_code = exception_code;
        query_log_element->exception = exception_message;
        if (context->getSettingsRef().calculate_text_stack_trace && exception_code != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            setExceptionStackTrace(*query_log_element);
        const auto time_now = std::chrono::system_clock::now();
        query_log_element->event_time = time_in_seconds(time_now);
        query_log_element->event_time_microseconds = time_in_microseconds(time_now);

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
        /// exception_handler will report failure plan segment status before release
        auto exception_handler = context->getPlanSegmentExHandler();
        if (exception_handler && exception_handler->setException(std::current_exception()))
            return convertFailurePlanSegmentStatusToResult(
                context,
                plan_segment_instance->info,
                exception_code,
                exception_message,
                std::move(final_progress),
                sender_metrics,
                plan_segment_outputs);
        return {};
    }

    //TODO notify segment scheduler with finished or exception status.
}

BlockIO PlanSegmentExecutor::lazyExecute(bool /*add_output_processors*/)
{
    LOG_DEBUG(&Poco::Logger::get("PlanSegmentExecutor"), "lazyExecute: {}", plan_segment->getPlanSegmentId());
    BlockIO res;
    // Will run as master query and already initialized
    if (!CurrentThread::get().getQueryContext() || CurrentThread::get().getQueryContext().get() != context.get())
        throw Exception("context not match", ErrorCodes::LOGICAL_ERROR);

    res.plan_segment_process_entry = context->getPlanSegmentProcessList().insertGroup(context, plan_segment->getPlanSegmentId());
    context->getPlanSegmentProcessList().insertProcessList(res.plan_segment_process_entry, *plan_segment, context);

    // set entry before buildPipeline to control memory usage of exchange queue
    context->setPlanSegmentProcessListEntry(res.plan_segment_process_entry);
    res.pipeline = std::move(*buildPipeline());
    return res;
}

void PlanSegmentExecutor::collectSegmentQueryRuntimeMetric(const QueryStatus * query_status)
{
    auto query_status_info = query_status->getInfo(true, context->getSettingsRef().log_profile_events);
    const auto & query_access_info = context->getQueryAccessInfo();

    query_log_element->read_bytes = query_status_info.read_bytes;
    query_log_element->read_rows = query_status_info.read_rows;
    query_log_element->disk_cache_read_bytes = query_status_info.disk_cache_read_bytes;
    query_log_element->written_bytes = query_status_info.written_bytes;
    query_log_element->written_rows = query_status_info.written_rows;
    query_log_element->memory_usage = query_status_info.peak_memory_usage > 0 ? query_status_info.peak_memory_usage : 0;
    query_log_element->query_duration_ms = query_status_info.elapsed_seconds * 1000;
    query_log_element->max_io_time_thread_ms = query_status_info.max_io_time_thread_ms;
    query_log_element->max_io_time_thread_name = query_status_info.max_io_time_thread_name;
    query_log_element->thread_ids = std::move(query_status_info.thread_ids);
    query_log_element->profile_counters = query_status_info.profile_counters;
    query_log_element->max_thread_io_profile_counters = query_status_info.max_io_thread_profile_counters;

    query_log_element->query_tables = query_access_info.tables;
}

StepProfiles collectStepRuntimeProfiles(const QueryPipelinePtr & pipeline)
{
    ProcessorProfiles profiles;
    for (const auto & processor : pipeline->getProcessors())
        profiles.push_back(std::make_shared<ProcessorProfile>(processor.get()));
    GroupedProcessorProfilePtr grouped_profiles = GroupedProcessorProfile::getGroupedProfiles(profiles);
    auto step_profile = GroupedProcessorProfile::aggregateOperatorProfileToStepLevel(grouped_profiles);
    AddressToStepProfile addr_to_step_profile;
    addr_to_step_profile["localhost"] = step_profile;
    return ProfileMetric::aggregateStepProfileBetweenWorkers(addr_to_step_profile);
}

void fillPlanSegmentProfile(
    PlanSegmentProfilePtr & segment_profile,
    const QueryPipelinePtr & pipeline,
    ReportProfileType type,
    const QueryStatus * query_status,
    ContextPtr context,
    PlanSegment * plan_segment)
{
    AddressInfo current_address = getLocalAddress(*context);
    segment_profile->worker_address = extractExchangeHostPort(current_address);
    if (query_status)
    {
        auto query_status_info = query_status->getInfo(true, context->getSettingsRef().log_profile_events);
        segment_profile->read_bytes = query_status_info.read_bytes;
        segment_profile->read_rows = query_status_info.read_rows;
        segment_profile->query_duration_ms = query_status_info.elapsed_seconds * 1000;
        segment_profile->io_wait_ms = query_status_info.max_io_time_thread_ms;
    }

    if (type == ReportProfileType::Unspecified)
        return;
    ProcessorProfiles profiles;
    for (const auto & processor : pipeline->getProcessors())
        profiles.push_back(std::make_shared<ProcessorProfile>(processor.get()));
    GroupedProcessorProfilePtr grouped_profiles = GroupedProcessorProfile::getGroupedProfiles(profiles);
    if (type == ReportProfileType::QueryPipeline)
    {
        auto output_root = GroupedProcessorProfile::getOutputRoot(grouped_profiles);
        segment_profile->profile_root_id = output_root->id;
        segment_profile->profiles = GroupedProcessorProfile::getProfileMetricsFromOutputRoot(output_root);
    }
    else if (type == ReportProfileType::QueryPlan)
    {
        auto step_profile = GroupedProcessorProfile::aggregateOperatorProfileToStepLevel(grouped_profiles);
        for (auto & [step_id, profile] : step_profile)
            segment_profile->profiles.emplace(step_id, profile);
        auto & plan = plan_segment->getQueryPlan();
        for (auto & node : plan.getNodes())
        {
            if (!node.step->getAttributeDescriptions().empty() && segment_profile->profiles.contains(node.id))
            {
                for (auto & att : node.step->getAttributeDescriptions())
                {
                    auto attribute_ptr = std::make_shared<RuntimeAttributeDescription>(att.second);
                    segment_profile->profiles.at(node.id)->attributes.emplace(att.first, attribute_ptr);
                }
            }
        }
    }
}

void PlanSegmentExecutor::doExecute()
{
    SCOPE_EXIT_SAFE({
        if (context->getSettingsRef().log_queries && process_plan_segment_entry->getQueryStatus())
            collectSegmentQueryRuntimeMetric(process_plan_segment_entry->getQueryStatus().get());
    });

    context->getPlanSegmentProcessList().insertProcessList(process_plan_segment_entry, *plan_segment, context);
    context->setPlanSegmentProcessListEntry(process_plan_segment_entry);

    if (context->getSettingsRef().bsp_mode)
    {
        auto query_unique_id = context->getCurrentTransactionID().toUInt64();
        auto instance_id = context->getPlanSegmentInstanceId();
        if (!context->getDiskExchangeDataManager()->cleanupPreviousSegmentInstance(query_unique_id, instance_id))
        {
            throw Exception(
                ErrorCodes::BSP_CLEANUP_PREVIOUS_SEGMENT_INSTANCE_FAILED,
                fmt::format(
                    "cleanup previous segment instance for query_unique_id:{} segment_id:{} parallel_id:{} failed",
                    query_unique_id,
                    plan_segment->getPlanSegmentId(),
                    plan_segment_instance->info.parallel_id));
        }
        CurrentThread::getProfileEvents().increment(ProfileEvents::PlanSegmentInstanceRetry, plan_segment_instance->info.retry_id);
    }

    // set process list before building pipeline, or else TableWriteTransform's output stream can't set its process list properly
    QueryStatus * query_status = process_plan_segment_entry->getQueryStatus().get();
    context->setProcessListElement(query_status);

    QueryPipelinePtr pipeline;
    BroadcastSenderPtrs senders;
    SCOPE_EXIT({
        if (pipeline)
            pipeline->clearUncompletedCache(context);
    });
    buildPipeline(pipeline, senders);

    pipeline->setProcessListElement(query_status);
    pipeline->setProgressCallback([&, ctx_progress_callback = context->getProgressCallback()](const Progress & value) {
        if (ctx_progress_callback)
            ctx_progress_callback(value);
        this->progress.incrementPiecewiseAtomically(value);
        this->final_progress.incrementPiecewiseAtomically(value);
    });

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

    PipelineExecutorPtr pipeline_executor;
    if (!context->getSettingsRef().interactive_delay_optimizer_mode)
    {
        pipeline_executor = pipeline->execute();
        pipeline_executor->execute(num_threads);
    }
    else
    {
        PullingAsyncPipelineExecutor async_pipeline_executor(*pipeline);
        Stopwatch after_send_progress;
        Block block;
        while (async_pipeline_executor.pull(block, context->getSettingsRef().interactive_delay_optimizer_mode / 1000))
        {
            if (after_send_progress.elapsed() / 1000 >= context->getSettingsRef().interactive_delay)
            {
                /// Some time passed and there is a progress.
                after_send_progress.restart();
                sendProgress();
            }
        }
        pipeline_executor = async_pipeline_executor.getPipelineExecutor();
    }

    pipeline->setWriteCacheComplete(context);

    if (CurrentThread::getGroup())
    {
        metrics.cpu_micros = CurrentThread::getGroup()->performance_counters[ProfileEvents::SystemTimeMicroseconds]
                + CurrentThread::getGroup()->performance_counters[ProfileEvents::UserTimeMicroseconds];
    }
    GraphvizPrinter::printPipeline(pipeline_executor->getProcessors(), pipeline_executor->getExecutingGraph(), context, plan_segment->getPlanSegmentId(), extractExchangeHostPort(plan_segment_instance->info.execution_address));
    for (const auto & sender : senders)
    {
        auto status = sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "Upstream pipeline finished");
        /// bsp mode will fsync data in finish, so we need to check if exception is thrown here.
        if (context->getSettingsRef().bsp_mode && status.code != BroadcastStatusCode::ALL_SENDERS_DONE)
            throw Exception(
                ErrorCodes::BSP_WRITE_DATA_FAILED,
                "Write data into disk failed in bsp mode, code {}, error message: {}",
                status.code,
                status.message);
    }


    if (context->getSettingsRef().bsp_mode)
    {
        for (const auto & sender : senders)
        {
            // TODO(WangTao): considering merge codition
            if (const auto sender_proxy = dynamic_pointer_cast<BroadcastSenderProxy>(sender))
            {
                const auto & key = sender_proxy->getDataKey();
                sender_metrics.bytes_sent[key->exchange_id].emplace_back(
                    key->partition_id, sender_proxy->getSenderMetrics().send_uncompressed_bytes.get_value());
            }
            else if (const auto writer = dynamic_pointer_cast<DiskPartitionWriter>(sender))
            {
                const auto & key = writer->getKey();
                sender_metrics.bytes_sent[key->exchange_id].emplace_back(
                    key->partition_id, writer->getSenderMetrics().send_uncompressed_bytes.get_value());
            }
        }
    }

    if (context->getSettingsRef().log_segment_profiles)
    {
        query_log_element->segment_profiles = std::make_shared<std::vector<String>>();
        query_log_element->segment_profiles->emplace_back(
            PlanSegmentDescription::getPlanSegmentDescription(plan_segment_instance->plan_segment, true)
                ->jsonPlanSegmentDescriptionAsString(collectStepRuntimeProfiles(pipeline)));
    }
    if (context->getSettingsRef().report_segment_profiles && plan_segment)
    {
        segment_profile = std::make_shared<PlanSegmentProfile>(query_log_element->client_info.initial_query_id, plan_segment->getPlanSegmentId());
        fillPlanSegmentProfile(segment_profile, pipeline, plan_segment->getProfileType(), query_status, context, plan_segment);
    }

    if (context->getSettingsRef().log_processors_profiles)
    {
        auto processors_profile_log = context->getProcessorsProfileLog();

        if (!processors_profile_log)
            return;

        processors_profile_log->addLogs(pipeline.get(),
                                        context->getClientInfo().initial_query_id,
                                        std::chrono::system_clock::now(),
                                        plan_segment->getPlanSegmentId());
    }
}

static QueryPlanOptimizationSettings buildOptimizationSettingsWithCheck(Poco::Logger * log, ContextMutablePtr& context)
{
    QueryPlanOptimizationSettings settings = QueryPlanOptimizationSettings::fromContext(context);
    if(!settings.enable_optimizer)
    {
        LOG_WARNING(log, "enable_optimizer should be true");
        settings.enable_optimizer = true;
    }
    return settings;
}

QueryPipelinePtr PlanSegmentExecutor::buildPipeline()
{
    QueryPipelinePtr pipeline = plan_segment->getQueryPlan().buildQueryPipeline(
        buildOptimizationSettingsWithCheck(logger, context),
        BuildQueryPipelineSettings::fromPlanSegment(plan_segment, plan_segment_instance->info, context));
    registerAllExchangeReceivers(logger, *pipeline, context->getSettingsRef().exchange_wait_accept_max_timeout_ms);
    return pipeline;
}

void PlanSegmentExecutor::buildPipeline(QueryPipelinePtr & pipeline, BroadcastSenderPtrs & senders)
{
    UInt64 current_tx_id = context->getCurrentTransactionID().toUInt64();
    std::vector<BroadcastSenderPtrs> senders_list;
    const auto & settings = context->getSettingsRef();
    auto disk_exchange_mgr = settings.bsp_mode ? context->getDiskExchangeDataManager() : nullptr; // get around with unittest
    auto sender_options
        = SenderProxyOptions{.wait_timeout_ms = settings.exchange_wait_accept_max_timeout_ms + settings.wait_runtime_filter_timeout};
    auto & sender_registry = BroadcastSenderProxyRegistry::instance();
    auto thread_group = CurrentThread::getGroup();

    /// need to create directory in bsp_mode before submitting write task
    if (settings.bsp_mode)
    {
        auto coordinator_address = extractExchangeHostPort(plan_segment->getCoordinatorAddress());
        disk_exchange_mgr->createWriteTaskDirectory(current_tx_id, plan_segment->getQueryId(), coordinator_address);
    }

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
        /// Exchange Sink will repartition data into 8 partition(2*4), partition id is range from 0 to 7.
        /// downstream plansegment and consumed partitions table:
        /// plansegment parallel_id :  partition id
        /// -----------------------------------------------
        /// 0                       : 0,1,2,3
        /// 1                       : 4,5,6,7
        size_t total_partition_num = exchange_parallel_size == 0 ? parallel_size : parallel_size * exchange_parallel_size;

        if (total_partition_num == 0)
            throw Exception("Total partition number should not be zero", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0; i < total_partition_num; i++)
        {
            size_t partition_id = i;
            auto data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id);
            BroadcastSenderPtr sender;
            if (settings.bsp_mode)
            {
                data_key->parallel_index = plan_segment_instance->info.parallel_id;
                auto writer = std::make_shared<DiskPartitionWriter>(context, disk_exchange_mgr, header, data_key);
                auto instance_id = context->getPlanSegmentInstanceId();
                disk_exchange_mgr->submitWriteTask(current_tx_id, instance_id, writer, thread_group);
                sender = writer;
            }
            else
            {
                auto proxy = sender_registry.getOrCreate(data_key, sender_options);
                proxy->accept(context, header);
                sender = proxy;
            }
            current_exchange_senders.emplace_back(std::move(sender));
        }

        senders_list.emplace_back(std::move(current_exchange_senders));
    }

    pipeline = plan_segment->getQueryPlan().buildQueryPipeline(
        buildOptimizationSettingsWithCheck(logger, context),
        BuildQueryPipelineSettings::fromPlanSegment(plan_segment, plan_segment_instance->info, context)
    );

    pipeline->setTotalsPortToMainPortTransform();
    pipeline->setExtremesPortToMainPortTransform();

    registerAllExchangeReceivers(logger, *pipeline, context->getSettingsRef().exchange_wait_accept_max_timeout_ms);

    pipeline->setMaxThreads(pipeline->getNumThreads());

    if (plan_segment->getPlanSegmentOutputs().empty())
        throw Exception("PlanSegment has no output", ErrorCodes::LOGICAL_ERROR);

    size_t sink_num = 0;
    auto max_output_size = std::max(context->getSettingsRef().max_threads.value / plan_segment_outputs.size(), 1UL);
    auto output_size = context->getSettingsRef().exchange_unordered_output_parallel_size.value;
    if (output_size > max_output_size)
    {
        LOG_DEBUG(logger, "Decrease plan_segment {} exchange output parallel size to {}", plan_segment->getPlanSegmentId(), max_output_size);
        output_size = max_output_size;
    }
    pipeline->limitMinThreads(output_size * plan_segment_outputs.size());
    for (size_t i = 0; i < plan_segment_outputs.size(); ++i)
    {
        const auto &cur_plan_segment_output = plan_segment_outputs[i];
        const auto &current_exchange_senders = senders_list[i];
        ExchangeMode exchange_mode = cur_plan_segment_output->getExchangeMode();
        bool keep_order = cur_plan_segment_output->needKeepOrder() || context->getSettingsRef().exchange_enable_force_keep_order;

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
                    "Cannot find expected ExchangeMode " + std::to_string(static_cast<UInt8>(exchange_mode)),
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
                            "Cannot find expected ExchangeMode " + std::to_string(static_cast<UInt8>(exchange_mode)),
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

void PlanSegmentExecutor::registerAllExchangeReceivers(Poco::Logger * log, const QueryPipeline & pipeline, UInt32 register_timeout_ms)
{
    const Processors & procesors = pipeline.getProcessors();
    std::vector<AsyncRegisterResult> async_results;
    std::vector<LocalBroadcastChannel *> local_receivers;
    std::vector<MultiPathReceiver *> multi_receivers;
    std::exception_ptr exception;

    try
    {
        for (const auto & processor : procesors)
        {
            auto exchange_source_ptr = std::dynamic_pointer_cast<ExchangeSource>(processor);
            if (!exchange_source_ptr)
                continue;
            auto * receiver_ptr = exchange_source_ptr->getReceiver().get();
            if (auto * brpc_receiver = dynamic_cast<BrpcRemoteBroadcastReceiver *>(receiver_ptr))
                async_results.emplace_back(brpc_receiver->registerToSendersAsync(register_timeout_ms));
            else if (auto * local_receiver = dynamic_cast<LocalBroadcastChannel *>(receiver_ptr))
                local_receivers.push_back(local_receiver);
            else if (auto * multi_receiver = dynamic_cast<MultiPathReceiver *>(receiver_ptr))
            {
                multi_receiver->registerToSendersAsync(register_timeout_ms);
                multi_receivers.push_back(multi_receiver);
            }
            else
                throw Exception("Unexpected SubReceiver Type: " + std::string(typeid(receiver_ptr).name()), ErrorCodes::LOGICAL_ERROR);
        }

        for (auto * receiver_ptr : local_receivers)
            receiver_ptr->registerToSenders(register_timeout_ms);
        for (auto * receiver_ptr : multi_receivers)
            receiver_ptr->registerToLocalSenders(register_timeout_ms);

        for (auto * receiver_ptr : multi_receivers)
            receiver_ptr->registerToSendersJoin();
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
                log,
                "Receiver register sender successfully but sender already finished, host: {}, request: {}",
                butil::endpoint2str(res.cntl->remote_side()).c_str(),
                *res.request);
            continue;
        }
        res.channel->assertController(*res.cntl, ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
        LOG_TRACE(
            log,
            "Receiver register sender successfully, host: {}, request: {}",
            butil::endpoint2str(res.cntl->remote_side()).c_str(),
            *res.request);
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
    auto repartition_func = RepartitionTransform::getRepartitionHashFunction(
        plan_segment_outputs[output_index]->getShuffleFunctionName(),
        arguments,
        context,
        plan_segment_outputs[output_index]->getShuffleFunctionParams());
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

void PlanSegmentExecutor::sendProgress()
{
    if (!progress.empty())
    {
        try
        {
            auto address = extractExchangeHostPort(plan_segment->getCoordinatorAddress());
            std::shared_ptr<RpcClient> rpc_client
                = RpcChannelPool::getInstance().getClient(address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
            Protos::PlanSegmentManagerService_Stub manager(&rpc_client->getChannel());
            brpc::Controller * cntl = new brpc::Controller;
            Protos::SendProgressRequest request;
            Protos::SendProgressResponse * response = new Protos::SendProgressResponse;
            request.set_query_id(plan_segment->getQueryId());
            request.set_segment_id(plan_segment->getPlanSegmentId());
            request.set_parallel_id(plan_segment_instance->info.parallel_id);
            *request.mutable_progress() = progress.fetchAndResetPiecewiseAtomically().toProto();
            cntl->set_timeout_ms(20000);
            manager.sendProgress(
                cntl,
                &request,
                response,
                brpc::NewCallback(
                    RPCHelpers::onAsyncCallDoneAssertController,
                    response,
                    cntl,
                    logger,
                    fmt::format(
                        "sendProgress failed for query_id:{} segment_id:{} parallel_id:{}",
                        plan_segment->getQueryId(),
                        plan_segment->getPlanSegmentId(),
                        plan_segment_instance->info.parallel_id)));
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}
}
