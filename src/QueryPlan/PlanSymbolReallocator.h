#pragma once

#include <Core/Names.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/MarkDistinctStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/SymbolMapper.h>

#include <functional>
#include <memory>
#include <unordered_map>

namespace DB
{

struct PlanNodeAndMappings
{
    PlanNodePtr plan_node;
    NameToNameMap mappings;
};

class PlanSymbolReallocator
{
public:
    /*
     * unalias symbol references that are just aliases of each other.
     * an projection will append to guarante output symbols.
     */
    static PlanNodePtr unalias(const PlanNodePtr & plan, ContextMutablePtr & context);

    /* 
     * deep copy a plan with symbol reallocated. 
     * symbol_mapping return all symbol reallocated mapping.
     * Note: output symbol names and order may changed.
     */
    static PlanNodeAndMappings reallocate(const PlanNodePtr & plan, ContextMutablePtr & context);

    /* check output stream is overlapping */
    static bool isOverlapping(const DataStream & lho, const DataStream & rho);
};

// Todo:
// - correlated subquery
// - conflicts check
class SymbolReallocatorVisitor : public PlanNodeVisitor<PlanNodeAndMappings, Void>
{
public:
    explicit SymbolReallocatorVisitor(
        ContextMutablePtr context_, std::function<SymbolMapper(std::unordered_map<String, String> & mapping)> && symbol_mapper_provider_)
        : context(context_), symbol_mapper_provider(symbol_mapper_provider_)
    {
    }

    PlanNodeAndMappings visitPlanNode(PlanNodeBase & node, Void & c) override
    {
        PlanNodes children;
        NameToNameMap mappings;
        for (auto & child : node.getChildren())
        {
            auto res = VisitorUtil::accept(child, *this, c);
            children.emplace_back(res.plan_node);
            mappings.insert(res.mappings.begin(), res.mappings.end());
        }
        auto symbol_mapper = symbol_mapper_provider(mappings);
        auto step = symbol_mapper.map(*node.getStep());

        auto plan_node = PlanNodeBase::createPlanNode(context->nextNodeId(), step, children, symbol_mapper.map(node.getStatistics()));
        return {plan_node, mappings};
    }

    PlanNodeAndMappings visitProjectionNode(ProjectionNode & project, Void & c) override
    {
        auto [child, mappings] = VisitorUtil::accept(project.getChildren()[0], *this, c);
        auto & step = project.getStep();

        // fix cases like array join copy column using projection
        std::unordered_map<std::string, size_t> referenced_expressions;
        for (const auto & assignment : step->getAssignments())
        {
            for (const auto & symbol : SymbolsExtractor::extract(assignment.second))
            {
                referenced_expressions[symbol]++;
            }
        }

        bool simple_alias = true;
        for (const auto & assignment : step->getAssignments())
        {
            if (const auto * identifier = assignment.second->as<ASTIdentifier>())
            {
                if (referenced_expressions[identifier->name()] <= 1 && !step->isFinalProject())
                {
                    if (assignment.first != identifier->name())
                        mappings[assignment.first] = identifier->name();
                    continue;
                }
            }

            simple_alias = false;
        }

        if (simple_alias)
            return {child, mappings};

        auto symbol_mapper = symbol_mapper_provider(mappings);

        auto plan_node = PlanNodeBase::createPlanNode(
            context->nextNodeId(), symbol_mapper.map(*project.getStep()), {child}, symbol_mapper.map(project.getStatistics()));

        if (step->isFinalProject())
        {
            mappings.clear();
            for (const auto & output : step->getAssignments())
                mappings[output.first] = output.first;
        }

        return {plan_node, mappings};
    }

    PlanNodeAndMappings visitJoinNode(JoinNode & join, Void & c) override
    {
        auto left = VisitorUtil::accept(join.getChildren()[0], *this, c);
        auto right = VisitorUtil::accept(join.getChildren()[1], *this, c);

        auto mappings = std::move(left.mappings);
        mappings.insert(right.mappings.begin(), right.mappings.end());

        auto symbol_mapper = symbol_mapper_provider(mappings);
        auto step = symbol_mapper.map(*join.getStep());
        if (step->getKind() == ASTTableJoin::Kind::Inner && step->getStrictness() != ASTTableJoin::Strictness::Asof)
        {
            for (size_t i = 0; i < step->getLeftKeys().size(); i++)
            {
                auto left_key_type = left.plan_node->getCurrentDataStream().header.getByName(step->getLeftKeys()[i]).type;
                auto right_key_type = right.plan_node->getCurrentDataStream().header.getByName(step->getRightKeys()[i]).type;
                if (left_key_type->equals(*right_key_type))
                {
                    mappings[step->getRightKeys()[i]] = step->getLeftKeys()[i];
                }
            }
        }

        step->setOutputStream(symbol_mapper.map(step->getOutputStream()));

        auto plan_node = PlanNodeBase::createPlanNode(
            context->nextNodeId(), step, {left.plan_node, right.plan_node}, symbol_mapper.map(join.getStatistics()));

        return {plan_node, computeOutputMappings(join, symbol_mapper)};
    }

    /**
     * IntersectOrExceptNode require input streams are the same.
     * TODO: remove this restricts for Intersect/Except/IntersectOrExceptNode
     */
    PlanNodeAndMappings visitIntersectOrExceptNode(IntersectOrExceptNode & intersect, Void & c) override
    {
        auto left = VisitorUtil::accept(intersect.getChildren()[0], *this, c);
        auto right = VisitorUtil::accept(intersect.getChildren()[1], *this, c);
        if (!isCompatibleHeader(left.plan_node->getCurrentDataStream().header, right.plan_node->getCurrentDataStream().header))
        {
            Assignments assignments;
            NameToType name_to_type;
            for (size_t i = 0; i < left.plan_node->getCurrentDataStream().header.columns(); i++)
            {
                const auto & output = left.plan_node->getCurrentDataStream().header.getByPosition(i);
                assignments.emplace_back(
                    output.name, std::make_shared<ASTIdentifier>(right.plan_node->getCurrentDataStream().header.getByPosition(i).name));
                name_to_type.emplace(output.name, output.type);
            }
            if (!Utils::isIdentity(assignments))
            {
                auto projection_step
                    = std::make_shared<ProjectionStep>(right.plan_node->getStep()->getOutputStream(), assignments, name_to_type);
                right.plan_node = PlanNodeBase::createPlanNode(context->nextNodeId(), projection_step, {right.plan_node});
            }
        }

        auto symbol_mapper = symbol_mapper_provider(left.mappings);
        auto plan_node = PlanNodeBase::createPlanNode(
            context->nextNodeId(),
            symbol_mapper.map(*intersect.getStep()),
            {left.plan_node, right.plan_node},
            symbol_mapper.map(intersect.getStatistics()));
        return {plan_node, computeOutputMappings(intersect, symbol_mapper)};
    }

    static NameToNameMap computeOutputMappings(PlanNodeBase & node, SymbolMapper & symbol_mapper)
    {
        NameToNameMap output_mappings;
        for (const auto & output : node.getStep()->getOutputStream().header)
            output_mappings.emplace(output.name, symbol_mapper.map(output.name));
        return output_mappings;
    }

private:
    ContextMutablePtr context;
    std::function<SymbolMapper(std::unordered_map<String, String> & mapping)> symbol_mapper_provider;
};
}
