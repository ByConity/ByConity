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

#include <memory>
#include <string>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/DataTrans/MultiPathBoundedQueue.h>
#include <Processors/Exchange/DataTrans/MultiPathReceiver.h>
#include <Processors/Exchange/DeserializeBufTransform.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/QueryPipeline.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/ISourceStep.h>
#include <QueryPlan/RemoteExchangeSourceStep.h>
#include <brpc/controller.h>
#include <butil/endpoint.h>
#include <Common/Exception.h>
#include <common/types.h>
#include <Interpreters/QueryExchangeLog.h>
#include <Interpreters/sendPlanSegment.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RemoteExchangeSourceStep::RemoteExchangeSourceStep(PlanSegmentInputs inputs_, DataStream input_stream_, bool is_add_totals_, bool is_add_extremes_)
    : ISourceStep(DataStream{.header = inputs_[0]->getHeader()}), inputs(std::move(inputs_)), is_add_totals(is_add_totals_), is_add_extremes(is_add_extremes_)
{
    input_streams.emplace_back(std::move(input_stream_));
    logger = &Poco::Logger::get("RemoteExchangeSourceStep");
}

void RemoteExchangeSourceStep::toProto(Protos::RemoteExchangeSourceStep & proto, bool) const
{
    // NOTE: this step is ISourceStep but not using serde of ISourceStep
    // maybe a bug, but here just follow the original serde anyway
    input_streams[0].toProto(*proto.mutable_input_stream());
    proto.set_step_description(step_description);
    for (auto & element : inputs)
    {
        if (!element)
            throw Exception("PlanSegmentInput cannot be nullptr", ErrorCodes::LOGICAL_ERROR);
        element->toProto(*proto.add_inputs());
    }
    proto.set_is_add_totals(is_add_totals);
    proto.set_is_add_extremes(is_add_extremes);
}

std::shared_ptr<RemoteExchangeSourceStep>
RemoteExchangeSourceStep::fromProto(const Protos::RemoteExchangeSourceStep & proto, ContextPtr context)
{
    DataStream input_stream;
    input_stream.fillFromProto(proto.input_stream());
    auto step_description = proto.step_description();

    PlanSegmentInputs inputs;
    for (auto & proto_element : proto.inputs())
    {
        auto element = std::make_shared<PlanSegmentInput>();
        element->fillFromProto(proto_element, context);
        inputs.emplace_back(std::move(element));
    }

    bool is_add_totals = proto.has_is_add_totals() ? proto.is_add_totals(): false;
    bool is_add_extremes = proto.has_is_add_extremes() ? proto.is_add_extremes(): false;

    auto step = std::make_unique<RemoteExchangeSourceStep>(inputs, input_stream, is_add_totals, is_add_extremes);
    step->setStepDescription(step_description);

    return step;
}

std::shared_ptr<IQueryPlanStep> RemoteExchangeSourceStep::copy(ContextPtr) const
{
    return std::make_shared<RemoteExchangeSourceStep>(inputs, input_streams[0], is_add_totals, is_add_extremes);
}

void RemoteExchangeSourceStep::setPlanSegment(PlanSegment * plan_segment_, ContextPtr context_)
{
    context = std::move(context_);
    plan_segment = plan_segment_;
    plan_segment_id = plan_segment->getPlanSegmentId();
    /// only plan segment at server needs to set totals source or extremes source
    if (plan_segment_id != 0)
    {
        is_add_totals = false;
        is_add_extremes = false;
    }
    query_id = plan_segment->getQueryId();
    coordinator_address = extractExchangeHostPort(plan_segment->getCoordinatorAddress());
    read_address_info = getLocalAddress(*context);
    if (!context)
        throw Exception("Plan segment not set context", ErrorCodes::BAD_ARGUMENTS);
    options = ExchangeUtils::getExchangeOptions(context);
}

void RemoteExchangeSourceStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto current_tx_id = context->getCurrentTransactionID().toUInt64();
    if (!plan_segment)
        throw Exception("Should setPlanSegment before initializePipeline!", ErrorCodes::LOGICAL_ERROR);

    Pipe pipe;

    size_t source_num = 0;
    bool keep_order = context->getSettingsRef().exchange_enable_force_keep_order || context->getSettingsRef().enable_shuffle_with_order;
    if (!keep_order)
    {
        for (const auto & input : inputs)
        {
            if (input->needKeepOrder())
            {
                keep_order = input->needKeepOrder();
                break;
            }
        }
    }

    const Block & exchange_header = getOutputStream().header;
    Block source_header;
    if (keep_order)
        source_header = exchange_header;

    ExchangeTotalsSourcePtr totals_source;
    if (is_add_totals)
        totals_source = std::make_shared<ExchangeTotalsSource>(source_header);
    ExchangeExtremesSourcePtr extremes_source;
    if (is_add_extremes)
        extremes_source = std::make_shared<ExchangeExtremesSource>(source_header);
    auto enable_metrics = context->getSettingsRef().log_query_exchange;
    auto query_exchange_log = enable_metrics ? context->getQueryExchangeLog(): nullptr;
    auto register_mode
        = context->getSettingsRef().bsp_mode ? BrpcExchangeReceiverRegistryService::DISK_READER : BrpcExchangeReceiverRegistryService::BRPC;
    auto disk_exchange_mgr = context->getSettingsRef().bsp_mode ? context->getDiskExchangeDataManager() : nullptr;
    size_t local_queue_size = context->getSettingsRef().exchange_local_receiver_queue_size;
    size_t remote_queue_size = context->getSettingsRef().exchange_remote_receiver_queue_size;
    size_t multi_path_queue_size = context->getSettingsRef().exchange_multi_path_receiver_queue_size;
    std::shared_ptr<MemoryController> memory_controller;
    auto weak_segment_process_list_entry = context->getPlanSegmentProcessListEntry().lock();
    if (weak_segment_process_list_entry)
        memory_controller = weak_segment_process_list_entry->getMemoryController();

    for (const auto & input : inputs)
    {
        size_t write_plan_segment_id = input->getPlanSegmentId();
        size_t exchange_parallel_size = input->getExchangeParallelSize();
        UInt32 exchange_id = input->getExchangeId();
        UInt32 parallel_id = context->getPlanSegmentInstanceId().parallel_id;
        auto exchange_mode = input->getExchangeMode();
        //TODO: hack logic for BROADCAST/LOCAL_NO_NEED_REPARTITION/LOCAL_MAY_NEED_REPARTITION, we should remove this logic
        if (exchange_mode == ExchangeMode::LOCAL_NO_NEED_REPARTITION || exchange_mode == ExchangeMode::LOCAL_MAY_NEED_REPARTITION)
            parallel_id = 0;
        else if (exchange_mode == ExchangeMode::BROADCAST)
            exchange_parallel_size = 1;
        size_t partition_id_start = parallel_id * exchange_parallel_size;
        if (context->getSettingsRef().exchange_enable_multipath_reciever && !keep_order)
        {
            LocalChannelOptions local_options{
                .queue_size = local_queue_size,
                .max_timeout_ts = options.exchange_timeout_ts,
                .enable_metrics = enable_metrics};
            if (input->getSourceAddress().empty() && !settings.distributed_settings.is_explain)
                throw Exception("No source address!", ErrorCodes::LOGICAL_ERROR);
            bool enable_block_compress = context->getSettingsRef().exchange_enable_block_compress;
            BroadcastReceiverPtrs receivers;
            MultiPathQueuePtr collector = std::make_shared<MultiPathBoundedQueue>(multi_path_queue_size, memory_controller);
            bool is_final_plan_segment = plan_segment_id == 0;
            size_t input_index = 0;
            for (const auto & source_address : input->getSourceAddress())
            {
                auto write_address = extractExchangeHostPort(source_address);
                for (size_t i = 0; i < exchange_parallel_size; ++i)
                {
                    UInt32 partition_id = partition_id_start + i;
                    ExchangeDataKeyPtr data_key;
                    if (context->getSettingsRef().bsp_mode)
                    {
                        if (exchange_mode == ExchangeMode::LOCAL_NO_NEED_REPARTITION
                            || exchange_mode == ExchangeMode::LOCAL_MAY_NEED_REPARTITION)
                        {
                            if (source_address.getHostName() == "localhost" && source_address.getPort() == 0)
                            {
                                data_key = std::make_shared<ExchangeDataKey>(
                                    current_tx_id, exchange_id, partition_id, context->getPlanSegmentInstanceId().parallel_id);
                            }
                            else
                            {
                                if (input_index == context->getPlanSegmentInstanceId().parallel_id)
                                    data_key = std::make_shared<ExchangeDataKey>(
                                        current_tx_id, exchange_id, partition_id, context->getPlanSegmentInstanceId().parallel_id);
                                else
                                    break;
                            }
                        }
                        else
                        {
                            data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id, input_index);
                        }
                    }
                    else
                    {
                        data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id);
                    }
                    bool is_local_exchange = ExchangeUtils::isLocalExchange(read_address_info, source_address);
                    BroadcastReceiverPtr receiver = createReceiver(
                        disk_exchange_mgr,
                        is_local_exchange,
                        local_options,
                        write_plan_segment_id,
                        exchange_id,
                        partition_id,
                        data_key,
                        exchange_header,
                        keep_order,
                        enable_metrics,
                        write_address,
                        collector,
                        register_mode,
                        query_exchange_log);
                    receivers.emplace_back(std::move(receiver));
                }
                input_index++;
            }
            if (settings.distributed_settings.is_explain)
            {
                ExchangeDataKeyPtr data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id_start);
                String name = BrpcRemoteBroadcastReceiver::generateName(
                            exchange_id, write_plan_segment_id, plan_segment_id, partition_id_start, coordinator_address);
                auto queue = std::make_shared<MultiPathBoundedQueue>(remote_queue_size, memory_controller);
                auto brpc_receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
                    std::move(data_key),
                    "",
                    context,
                    exchange_header,
                    keep_order,
                    name,
                    std::move(queue),
                    register_mode,
                    query_exchange_log);
                brpc_receiver->setEnableReceiverMetrics(enable_metrics);
                BroadcastReceiverPtr receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(brpc_receiver);
                receivers.emplace_back(std::move(receiver));
                source_num++;
            }
            String receiver_name = MultiPathReceiver::generateName(
                exchange_id, write_plan_segment_id, plan_segment_id, coordinator_address);
            auto multi_path_options
                = MultiPathReceiverOptions{.enable_block_compress = enable_block_compress, .enable_metrics = enable_metrics};
            auto multi_path_receiver = std::make_shared<MultiPathReceiver>(
                collector, std::move(receivers), exchange_header, receiver_name, std::move(multi_path_options), context);
            LOG_DEBUG(logger, "Create {}", multi_path_receiver->getName());
            auto source = std::make_shared<ExchangeSource>(source_header, std::move(multi_path_receiver), options, is_final_plan_segment, totals_source, extremes_source);
            pipe.addSource(std::move(source));
            source_num++;
        }
        else
        {
            LocalChannelOptions local_options{
                .queue_size = local_queue_size,
                .max_timeout_ts = options.exchange_timeout_ts,
                .enable_metrics = enable_metrics};
            if (input->getSourceAddress().empty() && !settings.distributed_settings.is_explain)
                throw Exception("No source address!", ErrorCodes::LOGICAL_ERROR);
            bool is_final_plan_segment = plan_segment_id == 0;
            size_t input_index = 0;
            for (const auto & source_address : input->getSourceAddress())
            {
                auto write_address = extractExchangeHostPort(source_address);
                for (size_t i = 0; i < exchange_parallel_size; ++i)
                {
                    size_t partition_id = partition_id_start + i;
                    ExchangeDataKeyPtr data_key;
                    if (context->getSettingsRef().bsp_mode)
                    {
                        if (exchange_mode == ExchangeMode::LOCAL_NO_NEED_REPARTITION
                            || exchange_mode == ExchangeMode::LOCAL_MAY_NEED_REPARTITION)
                        {
                            if (source_address.getHostName() == "localhost" && source_address.getPort() == 0)
                            {
                                data_key = std::make_shared<ExchangeDataKey>(
                                    current_tx_id, exchange_id, partition_id, context->getPlanSegmentInstanceId().parallel_id);
                            }
                            else
                            {
                                if (input_index == context->getPlanSegmentInstanceId().parallel_id)
                                    data_key = std::make_shared<ExchangeDataKey>(
                                        current_tx_id, exchange_id, partition_id, context->getPlanSegmentInstanceId().parallel_id);
                                else
                                    break;
                            }
                        }
                        else
                        {
                            data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id, input_index);
                        }
                    }
                    else
                    {
                        data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id);
                    }
                    bool is_local_exchange = ExchangeUtils::isLocalExchange(read_address_info, source_address);
                    BroadcastReceiverPtr receiver = createReceiver(
                        disk_exchange_mgr,
                        is_local_exchange,
                        local_options,
                        write_plan_segment_id,
                        exchange_id,
                        partition_id,
                        data_key,
                        exchange_header,
                        keep_order,
                        enable_metrics,
                        write_address,
                        nullptr,
                        register_mode,
                        query_exchange_log);
                    auto source = std::make_shared<ExchangeSource>(source_header, std::move(receiver), options, is_final_plan_segment);
                    pipe.addSource(std::move(source));
                    source_num++;
                }
                input_index++;
            }
            if (settings.distributed_settings.is_explain)
            {
                ExchangeDataKeyPtr data_key = std::make_shared<ExchangeDataKey>(current_tx_id, exchange_id, partition_id_start);
                String name = BrpcRemoteBroadcastReceiver::generateName(
                            exchange_id, write_plan_segment_id, plan_segment_id, partition_id_start, coordinator_address);
                auto brpc_receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
                    std::move(data_key),
                    "",
                    context,
                    exchange_header,
                    keep_order,
                    name,
                    std::make_shared<MultiPathBoundedQueue>(remote_queue_size, memory_controller));
                BroadcastReceiverPtr receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(brpc_receiver);
                auto source = std::make_shared<ExchangeSource>(source_header, std::move(receiver), options, is_final_plan_segment, totals_source, extremes_source);
                pipe.addSource(std::move(source));
                source_num++;
            }
        }
    }

    if (is_add_totals)
        pipe.addTotalsSource(std::move(totals_source));
    if (is_add_extremes)
        pipe.addExtremesSource(std::move(extremes_source));
    pipeline.init(std::move(pipe));
    if (!keep_order)
    {
        pipeline.resize(context->getSettingsRef().exchange_source_pipeline_threads);
        pipeline.addSimpleTransform([enable_compress = context->getSettingsRef().exchange_enable_block_compress, header = exchange_header](
                                        const Block &) { return std::make_shared<DeserializeBufTransform>(header, enable_compress); });
    }
    LOG_DEBUG(logger, "Total exchange source : {}, keep_order: {}", source_num, keep_order);
    pipeline.limitMinThreads(source_num);
    for (const auto & processor : pipeline.getProcessors())
        processors.emplace_back(processor);
}

BroadcastReceiverPtr RemoteExchangeSourceStep::createReceiver(
    DiskExchangeDataManagerPtr disk_mgr,
    bool is_local_exchange,
    const LocalChannelOptions & local_options,
    size_t write_plan_segment_id,
    size_t exchange_id,
    size_t partition_id,
    ExchangeDataKeyPtr data_key,
    const Block & exchange_header,
    bool keep_order,
    bool enable_metrics,
    const String & write_address,
    MultiPathQueuePtr collector,
    BrpcExchangeReceiverRegistryService::RegisterMode register_mode,
    std::shared_ptr<QueryExchangeLog> query_exchange_log)
{
    BroadcastReceiverPtr receiver;
    size_t remote_queue_size = context->getSettingsRef().exchange_remote_receiver_queue_size;
    std::shared_ptr<MemoryController> memory_controller;
    auto weak_segment_process_list_entry = context->getPlanSegmentProcessListEntry().lock();
    if (weak_segment_process_list_entry)
        memory_controller = weak_segment_process_list_entry->getMemoryController();
    if (is_local_exchange)
    {
        if (!options.force_remote_mode)
        {
            LOG_TRACE(
                logger,
                "Create local exchange source : {}@{} for plansegment {}->{}",
                *data_key,
                write_address,
                write_plan_segment_id,
                plan_segment_id);
            String name = LocalBroadcastChannel::generateName(
                exchange_id, write_plan_segment_id, plan_segment_id, partition_id, coordinator_address);
            auto queue = collector ? collector : std::make_shared<MultiPathBoundedQueue>(local_options.queue_size, memory_controller);
            auto local_channel = std::make_shared<LocalBroadcastChannel>(data_key, local_options, name, std::move(queue), context);
            receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(local_channel);
            if (context->getSettingsRef().bsp_mode)
            {
                /// we need to do this as to avoid previous sender waitBecomeRealSender in finish
                auto previous_sender = BroadcastSenderProxyRegistry::instance().get(data_key);
                if (previous_sender)
                {
                    previous_sender->finish(BroadcastStatusCode::SEND_CANCELLED, "cancelled as previous sender");
                    BroadcastSenderProxyRegistry::instance().remove(data_key);
                    LOG_WARNING(logger, "previous_sender found for query_id:{} key:{}", query_id, *data_key);
                }
                previous_sender = nullptr; // dont forget to release
                disk_mgr->cancelReadTask(data_key); /// cancel possible previous read task from last execution
                auto sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
                sender_proxy->accept(context, exchange_header);
                auto processors = disk_mgr->createProcessors(std::move(sender_proxy), exchange_header, context);
                disk_mgr->submitReadTask(query_id, data_key, std::move(processors), coordinator_address);
            }
        }
        else
        {
            String localhost_address = context->getHostWithPorts().getExchangeAddress();
            LOG_TRACE(
                logger,
                "Force local exchange use remote source : {}@{} for plansegment {}->{}",
                *data_key,
                localhost_address,
                write_plan_segment_id,
                plan_segment_id);
            String name = BrpcRemoteBroadcastReceiver::generateName(
                exchange_id, write_plan_segment_id, plan_segment_id, partition_id, coordinator_address);
            auto queue = collector ? collector
                                   : std::make_shared<MultiPathBoundedQueue>(remote_queue_size, memory_controller);
            auto brpc_receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
                std::move(data_key),
                localhost_address,
                context,
                exchange_header,
                keep_order,
                name,
                std::move(queue),
                register_mode,
                query_exchange_log,
                coordinator_address);
            brpc_receiver->setEnableReceiverMetrics(enable_metrics);
            receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(brpc_receiver);
        }
    }
    else
    {
        LOG_TRACE(
            logger,
            "Create remote exchange source : {}@{} for plansegment {}->{}",
            *data_key,
            write_address,
            write_plan_segment_id,
            plan_segment_id);
        String name = BrpcRemoteBroadcastReceiver::generateName(
            exchange_id, write_plan_segment_id, plan_segment_id, partition_id, coordinator_address);
        auto queue = collector ? collector
                               : std::make_shared<MultiPathBoundedQueue>(remote_queue_size, memory_controller);
        auto brpc_receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
            std::move(data_key),
            write_address,
            context,
            exchange_header,
            keep_order,
            name,
            std::move(queue),
            register_mode,
            query_exchange_log,
            coordinator_address);
        brpc_receiver->setEnableReceiverMetrics(enable_metrics);
        receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(brpc_receiver);
    }
    return receiver;
}

void RemoteExchangeSourceStep::describePipeline(FormatSettings & settings) const
{
    if (!inputs.empty())
        settings.out << String(settings.offset, settings.indent_char) << "Source segment_id : [ " << std::to_string(inputs.back().get()->getPlanSegmentId()) << " ]\n";
    ISourceStep::describePipeline(settings);
}

}
