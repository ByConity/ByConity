#pragma once
#include <QueryPlan/IQueryPlanStep.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class IntersectOrExceptStep : public IQueryPlanStep
{
public:
    using Operator = ASTSelectIntersectExceptQuery::Operator;
    using OperatorConverter = ASTSelectIntersectExceptQuery::OperatorConverter;

    /// max_threads is used to limit the number of threads for result pipeline.
    IntersectOrExceptStep(DataStreams input_streams_, Operator operator_, size_t max_threads_ = 0);

    String getName() const override { return "IntersectOrExcept"; }

    Type getType() const override { return Type::IntersectOrExcept; }

    void setInputStreams(const DataStreams & input_streams_) override { input_streams = input_streams_; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override { return std::make_shared<IntersectOrExceptStep>(input_streams, current_operator, max_threads); }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings) override;


    void toProto(Protos::IntersectOrExceptStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<IntersectOrExceptStep> fromProto(const Protos::IntersectOrExceptStep & proto, ContextPtr context);

    void describePipeline(FormatSettings & settings) const override;
    
    String getOperator() const;

private:
    Block header;
    Operator current_operator;
    size_t max_threads;
    Processors processors;
};

}
