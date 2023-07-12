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
#include <Interpreters/Context_fwd.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <boost/core/noncopyable.hpp>
#include <Poco/Logger.h>
#include <common/types.h>
#include <Protos/plan_segment_manager.pb.h>

namespace DB
{
class ThreadGroupStatus;
struct BlockIO;

namespace Protos
{
    class RuntimeSegmentsMetrics;
}

struct RuntimeSegmentsMetrics
{
    UInt64 cpu_micros;

    RuntimeSegmentsMetrics() : cpu_micros(0)
    {
    }

    RuntimeSegmentsMetrics(const Protos::RuntimeSegmentsMetrics & metrics_)
    {
        cpu_micros = metrics_.cpu_micros();
    }

    void setProtos(Protos::RuntimeSegmentsMetrics & metrics_) const
    {
        metrics_.set_cpu_micros(cpu_micros);
    }
};

struct RuntimeSegmentsStatus
{
    String query_id;
    int32_t segment_id;
    bool is_succeed;
    bool is_canceled;
    RuntimeSegmentsMetrics metrics;
    String message;
    int32_t code;
};

class PlanSegmentExecutor : private boost::noncopyable
{
public:
    explicit PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_);
    explicit PlanSegmentExecutor(PlanSegmentPtr plan_segment_, ContextMutablePtr context_, ExchangeOptions options_);

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
    PlanSegmentPtr plan_segment;
    PlanSegmentOutputs plan_segment_outputs;
    ExchangeOptions options;
    Poco::Logger * logger;
    RuntimeSegmentsStatus runtime_segment_status;
    std::unique_ptr<QueryLogElement> query_log_element;

    Processors buildRepartitionExchangeSink(BroadcastSenderPtrs & senders, bool keep_order, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    Processors buildBroadcastExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    Processors buildLoadBalancedExchangeSink(BroadcastSenderPtrs & senders, size_t output_index, const Block &header, OutputPortRawPtrs &ports);

    void sendSegmentStatus(const RuntimeSegmentsStatus & status) noexcept;

    void collectSegmentQueryRuntimeMetric(const QueryStatus * query_status);
    void prepareSegmentInfo() const;
};

}
