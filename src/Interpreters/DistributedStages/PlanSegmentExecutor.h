#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <boost/core/noncopyable.hpp>
#include <Poco/Logger.h>

namespace DB
{
class ThreadGroupStatus;
struct BlockIO;
struct RuntimeSegmentsStatus
{
    RuntimeSegmentsStatus(
        const String & queryId_, int32_t segmentId_, bool isSucceed_, bool isCanceled_, const String & message_, int32_t code_)
        : query_id(queryId_), segment_id(segmentId_), is_succeed(isSucceed_), is_canceled(isCanceled_), message(message_), code(code_)
    {
    }

    RuntimeSegmentsStatus() { }

    String query_id;
    int32_t segment_id;
    bool is_succeed;
    bool is_canceled;
    String message;
    int32_t code;
};

class PlanSegmentExecutor : private boost::noncopyable
{
public:
    explicit PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_);
    explicit PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_, ExchangeOptions options_);

    RuntimeSegmentsStatus execute(std::shared_ptr<ThreadGroupStatus> thread_group = nullptr);
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

    void sendSegmentStatus(const RuntimeSegmentsStatus & status) noexcept;
};

}
