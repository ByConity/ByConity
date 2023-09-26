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
#include <QueryPlan/ITransformingStep.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

enum class TotalsMode;

/// Execute HAVING and calculate totals. See TotalsHavingTransform.
class TotalsHavingStep : public ITransformingStep
{
public:
    TotalsHavingStep(
            const DataStream & input_stream_,
            bool overflow_row_,
            const ActionsDAGPtr & actions_dag_,
            const std::string & filter_column_,
            TotalsMode totals_mode_,
            double auto_include_threshold_,
            bool final_);

    TotalsHavingStep(
            const DataStream & input_stream_,
            bool overflow_row_,
            const ConstASTPtr & having_filter_,
            TotalsMode totals_mode_,
            double auto_include_threshold_,
            bool final_);

    String getName() const override { return "TotalsHaving"; }

    Type getType() const override { return Type::TotalsHaving; }

    bool isOverflowRow() const { return overflow_row; }
    String getFilterColumnName() const { return filter_column_name; }
    TotalsMode getTotalsMode() const { return totals_mode; }
    double getAutoIncludeThreshols() const { return auto_include_threshold; }
    bool isFinal() const { return final; }
    const ConstASTPtr & getHavingFilter() const
    {
        return having_filter;
    }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getActions() const { return actions_dag; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    void toProto(Protos::TotalsHavingStep & proto, bool for_hash_equals = false) const;

    static std::shared_ptr<TotalsHavingStep> fromProto(const Protos::TotalsHavingStep & proto, ContextPtr);

private:
    bool overflow_row;
    ActionsDAGPtr actions_dag;
    ConstASTPtr having_filter;
    String filter_column_name;
    TotalsMode totals_mode;
    double auto_include_threshold;
    bool final;
};

}

