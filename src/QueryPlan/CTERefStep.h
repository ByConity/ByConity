#pragma once

#include <QueryPlan/ISourceStep.h>
#include <QueryPlan/ProjectionStep.h>

#include <memory>

namespace DB
{
using CTEId = UInt32;
class CTEInfo;

/**
 * CTE is model as two parts: CTERef and CTEDef.
 * CTERefStep is a source node reference to CTEDef by id.
 * CTEDef is a virtual node, the plan is stored in QueryInfo.
 */
class CTERefStep : public ISourceStep
{
public:
    CTERefStep(DataStream output_, CTEId id_, std::unordered_map<String, String> output_columns_, ConstASTPtr filter_)
        : ISourceStep(std::move(output_)), id(id_), output_columns(std::move(output_columns_)), filter(std::move(filter_))
    {
    }

    CTEId getId() const { return id; }
    const std::unordered_map<String, String> & getOutputColumns() const { return output_columns; }
    std::unordered_map<String, String> getReverseOutputColumns() const;
    const ConstASTPtr & getFilter() const { return filter; }

    void initializePipeline(QueryPipeline &, const BuildQueryPipelineSettings &) override
    {
        throw Exception("Not supported", ErrorCodes::NOT_IMPLEMENTED);
    }
    String getName() const override { return "CTERef"; }
    Type getType() const override { return Type::CTERef; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const override;
    void serialize(WriteBuffer &) const override;

    std::shared_ptr<ProjectionStep> toProjectionStep() const;
    PlanNodePtr toInlinedPlanNode(CTEInfo & cte_info, ContextMutablePtr & context, bool with_filter = false) const;

private:
    /**
     * CTE id reference to CTEInfo in QueryPlan.
     */
    CTEId id;

    /**
     * Map of output column name to cte column name.
     */
    std::unordered_map<String, String> output_columns;

    /**
     * Filter pushed into CTE plan.
     * This is a hint used for generate inlined plan as there is a filter above CTERefStep.
     */
    ConstASTPtr filter;
};
}
