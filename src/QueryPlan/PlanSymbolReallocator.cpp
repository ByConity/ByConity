#include <Core/Names.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanSymbolReallocator.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/SymbolMapper.h>

namespace DB
{

class PlanSymbolReallocator::SymbolReallocatorVisitor : public PlanNodeVisitor<PlanNodePtr, SymbolMapper>
{
public:
    explicit SymbolReallocatorVisitor(ContextMutablePtr context_) : context(context_) { }

    PlanNodePtr visitPlanNode(PlanNodeBase & node, SymbolMapper & symbol_mapper) override
    {
        auto step = symbol_mapper.map(*node.getStep());
        PlanNodes children;
        for (auto & child : node.getChildren())
            children.emplace_back(VisitorUtil::accept(child, *this, symbol_mapper));
        return PlanNodeBase::createPlanNode(context->nextNodeId(), step, children, symbol_mapper.map(node.getStatistics()));
    }

    PlanNodePtr visitProjectionNode(ProjectionNode & project, SymbolMapper & symbol_mapper) override
    {
        auto step = symbol_mapper.map(*project.getStep());

        Assignments assignments;
        for (const auto & assignment : step->getAssignments())
        {
            if (const auto * identifier = assignment.second->as<ASTIdentifier>())
            {
                assignments.emplace(assignment.first, std::make_shared<ASTIdentifier>(assignment.first));
                symbol_mapper.addSymbolMapping(identifier->name(), assignment.first);
                continue;
            }
            assignments.emplace(assignment.first, assignment.second);
        }

        auto child = VisitorUtil::accept(project.getChildren()[0], *this, symbol_mapper);
        return PlanNodeBase::createPlanNode(
            context->nextNodeId(),
            std::make_shared<ProjectionStep>(
                child->getStep()->getOutputStream(),
                assignments,
                step->getNameToType(),
                step->isFinalProject(),
                step->getHints()),
            {child},
            symbol_mapper.map(project.getStatistics()));
    }

private:
    ContextMutablePtr context;
};


PlanNodePtr PlanSymbolReallocator::unalias(const PlanNodePtr & plan, ContextMutablePtr & context)
{
    SymbolReallocatorVisitor visitor{context};
    std::unordered_map<std::string, std::string> symbol_mapping;
    for (const auto & output_name: plan->getOutputNames()) {
        symbol_mapping.emplace(output_name, output_name);
    }
    SymbolMapper symbol_mapper = SymbolMapper::symbolMapper(symbol_mapping);
    return VisitorUtil::accept(plan, visitor, symbol_mapper);
}

PlanNodePtr
PlanSymbolReallocator::reallocate(const PlanNodePtr & plan, ContextMutablePtr & context, std::unordered_map<std::string, std::string> & symbol_mapping)
{
    SymbolReallocatorVisitor visitor{context};
    SymbolMapper symbol_mapper = SymbolMapper::symbolReallocator(symbol_mapping, *context->getSymbolAllocator());
    return VisitorUtil::accept(plan, visitor, symbol_mapper);
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
