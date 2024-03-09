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

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/QueryLog.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Protos/plan_segment_manager.pb.h>
#include <boost/core/noncopyable.hpp>
#include <Poco/Logger.h>
#include <common/types.h>

namespace DB
{
class ThreadGroupStatus;
struct BlockIO;

struct SenderMetrics
{
    std::unordered_map<size_t, std::vector<std::pair<UInt64, size_t>>> bytes_sent;
};

class PlanSegmentExecutor : private boost::noncopyable
{
public:
    explicit PlanSegmentExecutor(PlanSegmentInstancePtr plan_segment_instance_, ContextMutablePtr context_);
    explicit PlanSegmentExecutor(PlanSegmentInstancePtr plan_segment_instance_, ContextMutablePtr context_, ExchangeOptions options_);

    ~PlanSegmentExecutor() noexcept;

    RuntimeSegmentsStatus execute(std::shared_ptr<ThreadGroupStatus> thread_group = nullptr);
    BlockIO lazyExecute(bool add_output_processors = false);

    static void registerAllExchangeReceivers(const QueryPipeline & pipeline, UInt32 register_timeout_ms);

protected:
    void doExecute(std::shared_ptr<ThreadGroupStatus> thread_group);
    QueryPipelinePtr buildPipeline();
    void buildPipeline(QueryPipelinePtr & pipeline, BroadcastSenderPtrs & senders);

private:
    ContextMutablePtr context;
    PlanSegmentInstancePtr plan_segment_instance;
    PlanSegment * plan_segment;
    PlanSegmentOutputs plan_segment_outputs;
    ExchangeOptions options;
    Poco::Logger * logger;
    RuntimeSegmentsStatus runtime_segment_status;
    std::unique_ptr<QueryLogElement> query_log_element;
    SenderMetrics sender_metrics;

    Processors buildRepartitionExchangeSink(BroadcastSenderPtrs & senders, bool keep_order, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    Processors buildBroadcastExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    Processors buildLoadBalancedExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    void sendSegmentStatus(const RuntimeSegmentsStatus & status) noexcept;

    void collectSegmentQueryRuntimeMetric(const QueryStatus * query_status);
    void prepareSegmentInfo() const;
};

}
