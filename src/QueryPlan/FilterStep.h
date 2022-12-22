#pragma once
#include <QueryPlan/ITransformingStep.h>

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
    const String & getFilterColumnName() const { return filter_column_name; }
    bool removesFilterColumn() const { return remove_filter_column; }
    
    ActionsDAGPtr createActions(ContextPtr context, const ASTPtr & rewrite_filter) const;

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    ActionsDAGPtr actions_dag;
    ConstASTPtr filter;
    String filter_column_name;
    bool remove_filter_column;

    static ConstASTPtr rewriteDynamicFilter(const ConstASTPtr & filter, QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_context);
};

}
