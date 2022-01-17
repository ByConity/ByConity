#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <boost/core/noncopyable.hpp>
#include <Poco/Logger.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

class ThreadGroupStatus;
struct BlockIO;

class PlanSegmentExecutor : private boost::noncopyable
{
public:
    explicit PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_);
    explicit PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_, ExchangeOptions options_);

    void execute(std::shared_ptr<ThreadGroupStatus> thread_group = nullptr);
    BlockIO lazyExecute(bool add_output_processors = false);

protected:
    void doExecute(std::shared_ptr<ThreadGroupStatus> thread_group);
    QueryPipelinePtr buildPipeline();
    void buildPipeline(QueryPipelinePtr & pipeline, BroadcastSenderPtrs & senders);

private:
    ContextMutablePtr context;
    PlanSegmentPtr plan_segment;
    PlanSegmentOutputPtr plan_segment_output;
    ExchangeOptions options;
    Poco::Logger * logger;

    void addRepartitionExchangeSink(QueryPipelinePtr & pipeline, BroadcastSenderPtrs & senders, bool keep_order);

    void addBroadcastExchangeSink(QueryPipelinePtr & pipeline, BroadcastSenderPtrs & senders);

    void addLoadBalancedExchangeSink(QueryPipelinePtr & pipeline, BroadcastSenderPtrs & senders);

};

}
