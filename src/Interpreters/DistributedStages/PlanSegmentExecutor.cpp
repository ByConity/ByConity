#include <memory>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeRegistryCenter.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastRegistry.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/Exchange/LoadBalancedExchangeSink.h>
#include <Processors/Exchange/MultiPartitionExchangeSink.h>
#include <Processors/Exchange/RepartitionTransform.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Interpreters/ProcessList.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

PlanSegmentExecutor::PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_)
    : context(std::move(context_))
    , plan_segment(std::move(plan_segment_))
    , plan_segment_output(plan_segment->getPlanSegmentOutput())
    , logger(&Poco::Logger::get("PlanSegmentExecutor"))
{
    options = ExchangeUtils::getExchangeOptions(context);
}

PlanSegmentExecutor::PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_, ExchangeOptions options_)
    : context(std::move(context_))
    , plan_segment(std::move(plan_segment_))
    , plan_segment_output(plan_segment->getPlanSegmentOutput())
    , options(std::move(options_))
    , logger(&Poco::Logger::get("PlanSegmentExecutor"))
{
}

void PlanSegmentExecutor::execute(ThreadGroupStatusPtr thread_group)
{
    LOG_DEBUG(logger, "execute PlanSegment:\n" + plan_segment->toString());
    try
    {
        doExecute(std::move(thread_group));
    }
    catch (...)
    {
        tryLogCurrentException(logger, __PRETTY_FUNCTION__);
    }

    //TODO notify segment scheduler with finished or exception status.
}

BlockIO PlanSegmentExecutor::lazyExecute(bool add_output_processors)
{
    BlockIO res;
    // Will run as master query and already initialized
    if (!CurrentThread::get().getQueryContext() || CurrentThread::get().getQueryContext() != context)
        throw Exception("context not match", ErrorCodes::LOGICAL_ERROR);

    const String & query_id = plan_segment->getQueryId();
    const String & segment_id = std::to_string(plan_segment->getPlanSegmentId());
    //TODO: query_id should set by scheduler
    context->getClientInfo().initial_query_id = query_id;
    context->getClientInfo().current_query_id = query_id + "_" + segment_id;

    res.plan_segment_process_entry = context->getPlanSegmentProcessList().insert(*plan_segment, context);

    QueryStatus * query_status = & res.plan_segment_process_entry->get();
    context->setProcessListElement(query_status);
    res.pipeline = std::move(*buildPipeline(add_output_processors));
    res.pipeline.setProcessListElement(query_status);

    return res;
}

void PlanSegmentExecutor::doExecute(ThreadGroupStatusPtr thread_group)
{
    std::optional<CurrentThread::QueryScope> query_scope;

    if (!thread_group)
    {
        const String & query_id = plan_segment->getQueryId();
        const String & segment_id = std::to_string(plan_segment->getPlanSegmentId());
        //TODO: query_id should set by scheduler
        context->getClientInfo().initial_query_id = query_id;
        context->getClientInfo().current_query_id = query_id + "_" + segment_id;

        if (!CurrentThread::getGroup())
        {
            query_scope.emplace(context); // Running as master query and not initialized
        }
        else
        {
            // Running as master query and already initialized
            if (!CurrentThread::get().getQueryContext() || CurrentThread::get().getQueryContext() != context)
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

    QueryPipelinePtr pipeline = buildPipeline(true);

    QueryStatus * query_status = &process_plan_segment_entry->get();
    context->setProcessListElement(query_status);
    pipeline->setProcessListElement(query_status);

    auto pipeline_executor = pipeline->execute();

    size_t num_threads = pipeline->getNumThreads();

    LOG_TRACE(
        logger,
        "Runing plansegment id {}, segment: {} pipeline with {} threads",
        plan_segment->getQueryId(),
        plan_segment->getPlanSegmentId(),
        num_threads);
    pipeline_executor->execute(num_threads);
}


QueryPipelinePtr PlanSegmentExecutor::buildPipeline(bool add_output_processors)
{
    QueryPipelinePtr pipeline = plan_segment->getQueryPlan().buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));

    if (!add_output_processors)
    {
        return pipeline;
    }
    if (!plan_segment->getPlanSegmentOutput())
        throw Exception("PlanSegment has no output", ErrorCodes::LOGICAL_ERROR);

    size_t exchange_parallel_size = plan_segment_output->getExchangeParallelSize();
    ExchangeMode exchange_mode = plan_segment_output->getExchangeMode();
    size_t write_plan_segment_id = plan_segment->getPlanSegmentId();
    size_t read_plan_segment_id = plan_segment_output->getPlanSegmentId();
    String coordinator_address = extractExchangeStatusHostPort(plan_segment->getCoordinatorAddress());

    bool keep_order = plan_segment_output->needKeepOrder();

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
    size_t total_partition_num = plan_segment_output->getParallelSize() * exchange_parallel_size;

    if(total_partition_num == 0)
        throw Exception("Total partition number should not be zero", ErrorCodes::LOGICAL_ERROR);

    const Block & header = plan_segment_output->getHeader();
    LocalChannelOptions local_options{.queue_size = 50, .max_timeout_ms = options.exhcange_timeout_ms};

    BroadcastSenderPtrs senders;
    for (size_t i = 0; i < total_partition_num; i++)
    {
        size_t partition_id = i + 1;
        auto data_key = std::make_shared<ExchangeDataKey>(
            plan_segment->getQueryId(), write_plan_segment_id, read_plan_segment_id, partition_id, coordinator_address);
        BroadcastSenderPtr sender;
        if (options.local_debug_mode)
        {
            LOG_DEBUG(logger, "Create local sender: {}", data_key->dump());
            sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(*data_key, local_options);
        }
        else
        {
            LOG_DEBUG(logger, "Create remote sender: {}", data_key->dump());
            sender = BrpcExchangeRegistryCenter::getInstance().getOrCreateSender(std::vector<DataTransKeyPtr>{data_key}, context, header);
        }
        senders.emplace_back(std::move(sender));
    }

    if (!keep_order)
        pipeline->resize(context->getSettingsRef().exchange_output_parallel_size, false, false);

    if (exchange_mode == ExchangeMode::REPARTITION || exchange_mode == ExchangeMode::LOCAL_MAY_NEED_REPARTITION
        || exchange_mode == ExchangeMode::GATHER)
        addRepartitionExchangeSink(pipeline, senders, keep_order);
    else if (exchange_mode == ExchangeMode::LOCAL_NO_NEED_REPARTITION)
        addLoadBalancedExchangeSink(pipeline, senders);
    else if (exchange_mode == ExchangeMode::BROADCAST)
        addBroadcastExchangeSink(pipeline, senders);
    else
        throw Exception("Cannot find expected ExchangeMode " + std::to_string(UInt8(exchange_mode)), ErrorCodes::LOGICAL_ERROR);

    return pipeline;
}

void PlanSegmentExecutor::addRepartitionExchangeSink(QueryPipelinePtr & pipeline, BroadcastSenderPtrs senders, bool keep_order)
{
    ColumnsWithTypeAndName arguments;
    ColumnNumbers argument_numbers;
    for (const auto & column_name : plan_segment->getPlanSegmentOutput()->getShufflekeys())
    {
        arguments.emplace_back(plan_segment_output->getHeader().getByName(column_name));
        argument_numbers.emplace_back(plan_segment_output->getHeader().getPositionByName(column_name));
    }
    auto repartition_func = RepartitionTransform::getDefaultRepartitionFunction(arguments, context);

    if (keep_order)
    {
        //TODO
        throw Exception("Repartition exchange can not keep data order now", ErrorCodes::NOT_IMPLEMENTED);
    }
    else
    {
        pipeline->setSinks([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
            /// exchange sink only process StreamType::Main
            if (stream_type != QueryPipeline::StreamType::Main)
                return nullptr; /// return nullptr means this sink will not be added;
            return std::make_shared<MultiPartitionExchangeSink>(
                header, senders, repartition_func, argument_numbers, options);
        });
    }
}

void PlanSegmentExecutor::addBroadcastExchangeSink(QueryPipelinePtr & pipeline, BroadcastSenderPtrs senders)
{
    pipeline->setSinks([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
        if (stream_type != QueryPipeline::StreamType::Main)
            return nullptr;
        return std::make_shared<BroadcastExchangeSink>(header, senders);
    });
}

void PlanSegmentExecutor::addLoadBalancedExchangeSink(QueryPipelinePtr & pipeline, BroadcastSenderPtrs senders)
{
    pipeline->setSinks([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
        if (stream_type != QueryPipeline::StreamType::Main)
            return nullptr;
        return std::make_shared<LoadBalancedExchangeSink>(header, senders);
    });
}


}
