#pragma once

#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Optimizer/Property/Property.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{


class LocalExchangeStep : public ITransformingStep
{
public:
    explicit LocalExchangeStep(const DataStream & input_stream_, const ExchangeMode & mode_, Partitioning schema_);

    String getName() const override
    {
        return "LocalExchange";
    }

    Type getType() const override
    {
        return Type::LocalExchange;
    }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    const ExchangeMode & getExchangeMode() const
    {
        return exchange_type;
    }

    const Partitioning & getSchema() const
    {
        return schema;
    }

    Block getHeader() const
    {
        return getOutputStream().header;
    }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    void toProto(Protos::LocalExchangeStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<LocalExchangeStep> fromProto(const Protos::LocalExchangeStep & proto, ContextPtr context);

private:
    ExchangeMode exchange_type = ExchangeMode::UNKNOWN;
    Partitioning schema;
};


}
