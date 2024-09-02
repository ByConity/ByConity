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
#include <Core/SortDescription.h>
#include <Interpreters/prepared_statement.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{

/// Executes LIMIT. See LimitTransform.
class LimitStep : public ITransformingStep
{
public:
    LimitStep(
        const DataStream & input_stream_,
        SizeOrVariable limit_,
        SizeOrVariable offset_,
        bool always_read_till_end_ = false, /// Read all data even if limit is reached. Needed for totals.
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {},
        bool partial_ = false);

    String getName() const override { return "Limit"; }

    Type getType() const override { return Type::Limit; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    size_t getLimitForSorting() const
    {
        if (getLimitValue() > std::numeric_limits<UInt64>::max() - getOffsetValue())
            return 0;

        return getLimitValue() + getOffsetValue();
    }
    const SizeOrVariable & getLimit() const
    {
        return limit;
    }
    const SizeOrVariable & getOffset() const
    {
        return offset;
    }
    size_t getLimitValue() const
    {
        return std::get<size_t>(limit);
    }
    size_t getOffsetValue() const
    {
        return std::get<size_t>(offset);
    }
    bool hasPreparedParam() const
    {
        return std::holds_alternative<String>(limit) || std::holds_alternative<String>(offset);
    }
    bool isAlwaysReadTillEnd() const { return always_read_till_end; }
    bool isWithTies() const { return with_ties; }
    const SortDescription & getSortDescription() const { return description; }

    /// Change input stream when limit is pushed up. TODO: add clone() for steps.
    void updateInputStream(DataStream input_stream_);

    bool withTies() const { return with_ties; }
    bool isPartial() const { return partial; }

    void toProto(Protos::LimitStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<LimitStep> fromProto(const Protos::LimitStep & proto, ContextPtr);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    void prepare(const PreparedStatementContext & prepared_context) override;

private:
    SizeOrVariable limit;
    SizeOrVariable offset;
    bool always_read_till_end;

    bool with_ties;
    const SortDescription description;
    bool partial;
};

}
