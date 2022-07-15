#include <QueryPlan/CTERefStep.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSerDerHelper.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/PlanCopier.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{
std::shared_ptr<IQueryPlanStep> CTERefStep::copy(ContextPtr) const
{
    return std::make_shared<CTERefStep>(output_stream.value(), id, output_columns, filter);
}

void CTERefStep::serialize(WriteBuffer & buffer) const
{
    writeBinary(id, buffer);

    writeVarUInt(output_columns.size(), buffer);
    for (const auto & item : output_columns)
    {
        writeStringBinary(item.first, buffer);
        writeStringBinary(item.second, buffer);
    }

    serializeAST(filter->clone(), buffer);
}

std::shared_ptr<ProjectionStep> CTERefStep::toProjectionStep() const
{
    NamesAndTypes inputs;
    Assignments assignments;
    NameToType name_to_type;
    for (auto & item : output_columns)
        assignments.emplace_back(item.first, std::make_shared<ASTIdentifier>(item.second));
    for (auto & item : output_stream.value().header)
    {
        name_to_type.emplace(item.name, item.type);
        inputs.emplace_back(NameAndTypePair{output_columns.at(item.name), item.type});
    }
    return std::make_shared<ProjectionStep>(DataStream{inputs}, assignments, name_to_type);
}

PlanNodePtr CTERefStep::toInlinedPlanNode(CTEInfo & cte_info, ContextMutablePtr & context, bool with_filter) const
{
    auto with_clause_plan = PlanCopier::copy(cte_info.getCTEDef(id), context);
    if (with_filter)
    {
        auto filter_step = std::make_shared<FilterStep>(with_clause_plan->getStep()->getOutputStream(), filter);
        with_clause_plan = PlanNodeBase::createPlanNode(context->nextNodeId(), filter_step, {with_clause_plan});
    }
    return PlanNodeBase::createPlanNode(context->nextNodeId(), toProjectionStep(), {with_clause_plan});
}

std::unordered_map<String, String> CTERefStep::getReverseOutputColumns() const
{
    std::unordered_map<String, String> reverse;
    for (const auto & item : output_columns)
        reverse.emplace(item.second, item.first);
    return reverse;
}
}
