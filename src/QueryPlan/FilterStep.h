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

/// Implements WHERE, HAVING operations. See FilterTransform.
class FilterStep : public ITransformingStep
{
public:
    FilterStep(
        const DataStream & input_stream_,
        ActionsDAGPtr actions_dag_,
        String filter_column_name_,
        bool remove_filter_column_);

    FilterStep(const DataStream & input_stream_, const ConstASTPtr & filter_, bool remove_filter_column_ = true);

    String getName() const override { return "Filter"; }

    Type getType() const override { return Type::Filter; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    void updateInputStream(DataStream input_stream, bool keep_header);

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }
    const ConstASTPtr & getFilter() const { return filter; }
    void setFilter(ConstASTPtr new_filter) { filter = std::move(new_filter);}
    const String & getFilterColumnName() const { return filter_column_name; }
    bool removesFilterColumn() const { return remove_filter_column; }

    void toProto(Protos::FilterStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<FilterStep> fromProto(const Protos::FilterStep & proto, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    static ConstASTPtr rewriteRuntimeFilter(const ConstASTPtr & filter, QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_context);

    void prepare(const PreparedStatementContext & prepared_context) override;

    static std::pair<ConstASTPtr, ConstASTPtr> splitLargeInValueList(const ConstASTPtr & filter, UInt64 limit);
    static std::vector<ConstASTPtr> removeLargeInValueList(const std::vector<ConstASTPtr> & filters, UInt64 limit);
private:
    ActionsDAGPtr actions_dag;
    ConstASTPtr filter;
    String filter_column_name;
    bool remove_filter_column;

};

}
