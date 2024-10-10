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

#include <Common/Logger.h>
#include <memory>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <QueryPlan/ISourceStep.h>
#include <Poco/Logger.h>

namespace DB
{
class PlanSegmentInput;
using PlanSegmentInputPtr = std::shared_ptr<PlanSegmentInput>;
using PlanSegmentInputs = std::vector<PlanSegmentInputPtr>;

class PlanSegment;

class RemoteExchangeSourceStepXXX
{
    DataStream input_stream;
    String step_description;
    PlanSegmentInputs inputs;
};
class RemoteExchangeSourceStep : public ISourceStep
{
public:
    explicit RemoteExchangeSourceStep(PlanSegmentInputs inputs_, DataStream input_stream_, bool is_add_totals_, bool is_add_extremes_);

    String getName() const override { return "RemoteExchangeSource"; }
    Type getType() const override { return Type::RemoteExchangeSource; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    PlanSegmentInputs getInput() const { return inputs; }
    void setInputs(PlanSegmentInputs inputs_)
    {
        inputs = std::move(inputs_);
    }
    void setInputStream(DataStream input_stream_)
    {
        input_streams = {std::move(input_stream_)};
    }

    void setPlanSegment(PlanSegment * plan_segment_, ContextPtr context_);
    PlanSegment * getPlanSegment() const { return plan_segment; }
    size_t getPlanSegmentId() const { return plan_segment_id; }

    void toProto(Protos::RemoteExchangeSourceStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<RemoteExchangeSourceStep> fromProto(const Protos::RemoteExchangeSourceStep & proto, ContextPtr context);

    void describePipeline(FormatSettings & settings) const override;

    void setExchangeOptions(ExchangeOptions options_) { options = options_; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;

    bool isAddTotals() const { return is_add_totals; }
    bool isAddExtremes() const  { return is_add_extremes; }

private:
    void registerAllReceivers(BrpcReceiverPtrs receivers, UInt32 timeout_ms);
    BroadcastReceiverPtr createReceiver(
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
        const String & write_address_info,
        MultiPathQueuePtr collector,
        BrpcExchangeReceiverRegistryService::RegisterMode register_mode,
        std::shared_ptr<QueryExchangeLog> query_exchange_log);
    PlanSegmentInputs inputs;
    PlanSegment * plan_segment = nullptr;
    LoggerPtr logger;
    size_t plan_segment_id;
    String query_id;
    String coordinator_address;
    AddressInfo read_address_info;
    ContextPtr context;
    ExchangeOptions options;
    bool is_add_totals;
    bool is_add_extremes;
};
}
