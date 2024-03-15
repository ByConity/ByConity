#pragma once

#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/TableWriteStep.h>

namespace DB
{
class TableFinishStep : public ITransformingStep
{
public:
    TableFinishStep(const DataStream & input_stream_, TableWriteStep::TargetPtr target_, String output_affected_row_count_symbol_, ASTPtr query_);

    String getName() const override
    {
        return "TableFinish";
    }
    Type getType() const override
    {
        return Type::TableFinish;
    }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;
    void setInputStreams(const DataStreams & input_streams_) override
    {
        input_streams = input_streams_;
        output_stream = DataStream{.header = std::move((input_streams_[0].header))};
    }

    TableWriteStep::TargetPtr getTarget() const
    {
        return target;
    }

    const String & getOutputAffectedRowCountSymbol() const { return output_affected_row_count_symbol; }

    void setQuery(const ASTPtr & query_) { query = query_; }
    ASTPtr getQuery() const { return query; }

    void toProto(Protos::TableFinishStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<TableFinishStep> fromProto(const Protos::TableFinishStep & proto, ContextPtr context);

private:
    TableWriteStep::TargetPtr target;
    String output_affected_row_count_symbol;
    ASTPtr query;
    Poco::Logger * log;
};
}
