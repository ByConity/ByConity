#pragma once
#include <Core/Names.h>
#include <Core/Types.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/PlanVisitor.h>

namespace DB
{

static auto getIntersection = [](const NameOrderedSet & names_a, const NameOrderedSet & names_b) {
    NameOrderedSet result;
    std::set_intersection(names_a.begin(), names_a.end(), names_b.begin(), names_b.end(), std::inserter(result, result.end()));
    return result;
};

static auto isSubset = [](const NameOrderedSet & names_a, const NameOrderedSet & names_b) {
    return getIntersection(names_a, names_b).size() == names_a.size();
};

static auto isSubNameSet = [](const NameSet & names_a, const NameSet & names_b) {
    return std::all_of(names_a.begin(), names_b.end(), [&](const String & str) { return names_b.contains(str); });
};

using NamesAndFunctions = std::vector<std::pair<String, ConstASTPtr>>;
class CollectIncludeFunction : public ConstASTVisitor<ConstASTPtr, NamesAndFunctions>
{
public:
    explicit CollectIncludeFunction(const NameSet & expected_funcs_, ContextMutablePtr context_)
        : expected_funcs(expected_funcs_), context(context_)
    {
    }

    ConstASTPtr visitASTFunction(const ConstASTPtr & node, NamesAndFunctions & assignments) override
    {
        visitNode(node, assignments);

        if (const auto * func = node->as<ASTFunction>())
        {
            String func_name = Poco::toLower(func->name);
            if (expected_funcs.contains(func_name))
            {
                assignments.emplace_back(func_name, node->clone());
            }
        }
        return node;
    }

    ConstASTPtr visitNode(const ConstASTPtr & node, NamesAndFunctions & c) override
    {
        for (const ASTPtr & child : node->children)
        {
            ASTVisitorUtil::accept(child, *this, c);
        }
        return node;
    }

    static NamesAndFunctions collect(ConstASTPtr ast, const NameSet & expected_funcs, ContextMutablePtr context)
    {
        CollectIncludeFunction rewriter{expected_funcs, context};
        NamesAndFunctions assignments;
        ASTVisitorUtil::accept(ast, rewriter, assignments);
        return assignments;
    }

private:
    NameSet expected_funcs;
    ContextMutablePtr context;
};

class CollectExcludeFunction : public ConstASTVisitor<ConstASTPtr, NamesAndFunctions>
{
public:
    explicit CollectExcludeFunction(const NameSet & unexpected_funcs_, ContextMutablePtr context_)
        : unexpected_funcs(unexpected_funcs_), context(context_)
    {
    }

    ConstASTPtr visitASTFunction(const ConstASTPtr & node, NamesAndFunctions & assignments) override
    {
        visitNode(node, assignments);

        if (const auto * func = node->as<ASTFunction>())
        {
            String func_name = Poco::toLower(func->name);
            if (!unexpected_funcs.contains(func_name))
            {
                assignments.emplace_back(func_name, node->clone());
            }
        }
        return node;
    }

    ConstASTPtr visitNode(const ConstASTPtr & node, NamesAndFunctions & c) override
    {
        for (const ASTPtr & child : node->children)
        {
            ASTVisitorUtil::accept(child, *this, c);
        }
        return node;
    }

    static NamesAndFunctions collect(ConstASTPtr ast, const NameSet & unexpected_funcs, ContextMutablePtr context)
    {
        CollectExcludeFunction rewriter{unexpected_funcs, context};
        NamesAndFunctions assignments;
        ASTVisitorUtil::accept(ast, rewriter, assignments);
        return assignments;
    }

private:
    NameSet unexpected_funcs;
    ContextMutablePtr context;
};

class CollectPlanNodeVisitor : public PlanNodeVisitor<Void, PlanNodes>
{
public:
    explicit CollectPlanNodeVisitor(
        const std::unordered_set<IQueryPlanStep::Type> & expected_steps_, CTEInfo & cte_info_, ContextMutablePtr context_)
        : expected_steps(expected_steps_), cte_info(cte_info_), context(context_)
    {
    }

    Void visitPlanNode(PlanNodeBase & node, PlanNodes & c) override
    {
        for (const auto & child : node.getChildren())
        {
            VisitorUtil::accept(*child, *this, c);
        }

        auto type = node.getStep()->getType();
        if (expected_steps.contains(type))
            c.emplace_back(node.shared_from_this());
        return Void{};
    }

    Void visitCTERefNode(CTERefNode & node, PlanNodes & c) override
    {
        auto cte_def = cte_info.getCTEDef(node.getStep()->getId());
        return VisitorUtil::accept(*cte_def, *this, c);
    }

    static PlanNodes collect(
        PlanNodePtr plan_node,
        const std::unordered_set<IQueryPlanStep::Type> & expected_steps,
        CTEInfo & cte_info,
        ContextMutablePtr context)
    {
        CollectPlanNodeVisitor rewriter{expected_steps, cte_info, context};
        PlanNodes plan_nodes;
        VisitorUtil::accept(plan_node, rewriter, plan_nodes);
        return plan_nodes;
    }

private:
    std::unordered_set<IQueryPlanStep::Type> expected_steps;
    CTEInfo & cte_info;
    ContextMutablePtr context;
};

template <typename R, typename C>
class StepCTEVisitHelper
{
public:
    explicit StepCTEVisitHelper(CTEInfo & cte_info_) : cte_info(cte_info_)
    {
    }

    R accept(CTEId id, StepVisitor<R, C> & visitor, C & context)
    {
        auto it = visit_results.find(id);
        if (it != visit_results.end())
        {
            return it->second;
        }
        auto res = VisitorUtil::accept(cte_info.getCTEDef(id)->getStep(), visitor, context);
        visit_results.emplace(id, res);
        return res;
    }

    CTEInfo & getCTEInfo()
    {
        return cte_info;
    }
    bool hasVisited(CTEId cte_id)
    {
        return visit_results.contains(cte_id);
    }

private:
    CTEInfo & cte_info;
    std::unordered_map<CTEId, R> visit_results;
    std::unordered_map<CTEId, C> visit_contexts;
};

template <typename R, typename C>
class NodeCTEVisitHelper
{
public:
    explicit NodeCTEVisitHelper(CTEInfo & cte_info_) : cte_info(cte_info_)
    {
    }

    R accept(CTEId id, PlanNodeVisitor<R, C> & visitor, C & context)
    {
        auto it = visit_results.find(id);
        if (it != visit_results.end())
        {
            context = visit_contexts.at(id);
            return it->second;
        }
        auto res = VisitorUtil::accept(cte_info.getCTEDef(id), visitor, context);
        visit_results.emplace(id, res);
        visit_contexts.emplace(id, context);
        return res;
    }

    CTEInfo & getCTEInfo()
    {
        return cte_info;
    }
    bool hasVisited(CTEId cte_id)
    {
        return visit_results.contains(cte_id);
    }

private:
    CTEInfo & cte_info;
    std::unordered_map<CTEId, R> visit_results;
    std::unordered_map<CTEId, C> visit_contexts;
};

}
