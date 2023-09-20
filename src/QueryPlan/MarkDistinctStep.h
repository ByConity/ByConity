#pragma once

#include <Core/NameToType.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class MarkDistinctStep : public ITransformingStep
{
public:
    explicit MarkDistinctStep(
        const DataStream & input_stream_,
        String marker_symbol_,
        std::vector<String> distinct_symbols_);

    String getName() const override { return "MarkDistinct"; }
    Type getType() const override { return Type::MarkDistinct; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
    void toProto(Protos::MarkDistinctStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<MarkDistinctStep> fromProto(const Protos::MarkDistinctStep & proto, ContextPtr);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    String getMarkerSymbol() const { return marker_symbol;}
    const std::vector<String> & getDistinctSymbols() const {return distinct_symbols;}

private:
    String marker_symbol;
    std::vector<String> distinct_symbols;
};

}
