#include <Core/NameToType.h>
#include <Core/Names.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanSymbolReallocator.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/SymbolMapper.h>
#include <Common/Exception.h>

namespace DB
{

PlanNodePtr PlanSymbolReallocator::unalias(const PlanNodePtr & plan, ContextMutablePtr & context)
{
    auto origin_output_stream = plan->getStep()->getOutputStream();

    SymbolReallocatorVisitor visitor{context, [](auto & mapping) { return SymbolMapper::symbolMapper(mapping); }};
    Void c;
    auto [plan_node, mappings] = VisitorUtil::accept(plan, visitor, c);
    auto symbol_mapper = SymbolMapper::symbolMapper(mappings);

    // create projection step to guarante output symbols as output symbol size and order may changed in SymbolReallocatorVisitor
    Assignments assignments;
    NameToType name_to_type;
    for (const auto & output : origin_output_stream.header)
    {
        assignments.emplace_back(output.name, std::make_shared<ASTIdentifier>(symbol_mapper.map(output.name)));
        name_to_type.emplace(output.name, output.type);
    }
    if (!Utils::isIdentity(assignments))
    {
        auto projection_step = std::make_shared<ProjectionStep>(plan_node->getStep()->getOutputStream(), assignments, name_to_type);
        plan_node = PlanNodeBase::createPlanNode(context->nextNodeId(), projection_step, {plan_node});
    }

    return plan_node;
}

PlanNodeAndMappings PlanSymbolReallocator::reallocate(const PlanNodePtr & plan, ContextMutablePtr & context)
{
    Names origin_outputs = plan->getOutputNames();

    SymbolReallocatorVisitor visitor{
        context, [&](auto & mapping) { return SymbolMapper::symbolReallocator(mapping, *context->getSymbolAllocator()); }};
    Void c;
    auto [plan_node, mappings] = VisitorUtil::accept(plan, visitor, c);
    auto symbol_mapper = SymbolMapper::symbolMapper(mappings);

    NameToNameMap output_symbol_mappings;
    for (const auto & output : origin_outputs)
        output_symbol_mappings.emplace(output, symbol_mapper.map(output));
    return {plan_node, output_symbol_mappings};
}

bool PlanSymbolReallocator::isOverlapping(const DataStream & lho, const DataStream & rho)
{
    NameSet name_set;
    std::transform(lho.header.begin(), lho.header.end(), std::inserter(name_set, name_set.end()), [](const auto & nameAndType) {
        return nameAndType.name;
    });

    return std::any_of(rho.header.begin(), rho.header.end(), [&](const auto & nameAndType) { return name_set.contains(nameAndType.name); });
}

}
