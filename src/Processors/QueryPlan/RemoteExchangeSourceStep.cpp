#include <memory>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeRegistryCenter.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastRegistry.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/QueryPipeline.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/RemoteExchangeSourceStep.h>
#include <Common/Exception.h>
#include "Processors/Sources/NullSource.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RemoteExchangeSourceStep::RemoteExchangeSourceStep(PlanSegmentInputs inputs_, DataStream input_stream_)
    : ISourceStep(DataStream{.header = inputs_[0]->getHeader()}), inputs(std::move(inputs_))
{
    input_streams.emplace_back(std::move(input_stream_));
    logger = &Poco::Logger::get("RemoteExchangeSourceStep");
}

void RemoteExchangeSourceStep::serialize(WriteBuffer & buf) const
{
    IQueryPlanStep::serializeImpl(buf);

    writeBinary(inputs.size(), buf);
    for (const auto & input : inputs)
        input->serialize(buf);
}

QueryPlanStepPtr RemoteExchangeSourceStep::deserialize(ReadBuffer & buf, ContextPtr context)
{
    String step_description;
    readBinary(step_description, buf);

    auto input_stream = deserializeDataStream(buf);

    size_t input_size;
    readBinary(input_size, buf);
    PlanSegmentInputs inputs(input_size);
    for (size_t i = 0; i < input_size; ++i)
    {
        inputs[i] = std::make_shared<PlanSegmentInput>();
        inputs[i]->deserialize(buf, context);
    }

    auto step = std::make_unique<RemoteExchangeSourceStep>(inputs, input_stream);
    step->setStepDescription(step_description);
    return step;
}

void RemoteExchangeSourceStep::setPlanSegment(PlanSegment * plan_segment_)
{
    plan_segment = plan_segment_;
    plan_segment_id = plan_segment->getPlanSegmentId();
    query_id = plan_segment->getQueryId();
    coordinator_address = extractExchangeStatusHostPort(plan_segment->getCoordinatorAddress());
    context = plan_segment->getContext();
    if (!context)
        throw Exception("Plan segment not set context", ErrorCodes::BAD_ARGUMENTS);
    options = ExchangeUtils::getExchangeOptions(context);
}

void RemoteExchangeSourceStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    if (!plan_segment)
        throw Exception("Should setPlanSegment before initializePipeline!", ErrorCodes::LOGICAL_ERROR);

    Pipe pipe;

    const Block & header = getOutputStream().header;

    for (const auto & input : inputs)
    {
        size_t write_plan_segment_id = input->getPlanSegmentId();
        size_t exchange_parallel_size = input->getExchangeParallelSize();

        //TODO: hack logic for BROADCAST, we should remove this logic
        if (input->getExchangeMode() == ExchangeMode::BROADCAST)
            exchange_parallel_size = 1;

        size_t partition_id_start = (input->getParallelIndex() - 1) * exchange_parallel_size + 1;
        if (options.local_debug_mode)
        {
            LocalChannelOptions local_options{.queue_size = 50, .max_timeout_ms = options.exhcange_timeout_ms};

            for (size_t i = 0; i < exchange_parallel_size; ++i)
            {
                size_t partition_id = partition_id_start + i;
                DataTransKeyPtr data_key = std::make_shared<ExchangeDataKey>(
                    query_id, write_plan_segment_id, plan_segment_id, partition_id, coordinator_address);
                BroadcastReceiverPtr receiver;
                LOG_DEBUG(logger, "Create local exchange source : {}", data_key->dump());
                receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(*data_key, local_options);
                auto source = std::make_shared<ExchangeSource>(header, std::move(receiver), options);
                pipe.addSource(std::move(source));
            }
        }
        else
        {
            if (input->getSourceAddress().empty())
                throw Exception("No source address!", ErrorCodes::LOGICAL_ERROR);

            for (const auto & source_address : input->getSourceAddress())
            {
                const auto & write_address_info = extractExchangeHostPort(source_address);
                for (size_t i = 0; i < exchange_parallel_size; ++i)
                {
                    size_t partition_id = partition_id_start + i;
                    DataTransKeyPtr data_key = std::make_shared<ExchangeDataKey>(
                        query_id, write_plan_segment_id, plan_segment_id, partition_id, coordinator_address);
                    BroadcastReceiverPtr receiver;
                    LOG_DEBUG(logger, "Create remote exchange source : {}@{}", data_key->dump(), write_address_info);
                    receiver = BrpcExchangeRegistryCenter::getInstance().getOrCreateReceiver(data_key, write_address_info, context, header);
                    auto source = std::make_shared<ExchangeSource>(header, std::move(receiver), options);
                    pipe.addSource(std::move(source));
                }
            }
            pipe.addSource(std::make_shared<NullSource>(header));
        }
    }
    
    pipeline.init(std::move(pipe));
}


void RemoteExchangeSourceStep::describePipeline(FormatSettings & /*settings*/) const {
    //TODO
};


}
