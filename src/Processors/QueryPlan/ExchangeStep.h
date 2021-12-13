#pragma once

#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/DistributedStages/Property.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

class ExchangeStep : public IQueryPlanStep
{
public:
    explicit ExchangeStep(DataStreams input_streams_, const ExchangeMode & mode_,  Partitioning schema_, bool keep_order_ = false);

    String getName() const override { return "Exchange"; }

    Type getType() const override { return Type::Exchange; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & context) override;

    void serialize(WriteBuffer & buf) const override;

    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr & context);

    const ExchangeMode & getExchangeMode() const { return exchange_type; }

    const Partitioning & getSchema() const { return schema; }

    bool needKeepOrder() const { return keep_order; }

    Block getHeader() const { return getOutputStream().header; }

private:
    ExchangeMode exchange_type = ExchangeMode::UNKNOWN;
    Partitioning schema;
    bool keep_order = false;
};


}
