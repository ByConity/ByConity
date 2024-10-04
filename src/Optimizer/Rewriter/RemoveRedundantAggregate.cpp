#include <Optimizer/Rewriter/RemoveRedundantAggregate.h>

#include <Optimizer/SymbolsExtractor.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/UnionStep.h>
#include <Optimizer/PredicateUtils.h>
#include "Parsers/IAST_fwd.h"

namespace DB
{
bool RemoveRedundantDistinct::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    RemoveRedundantAggregateVisitor visitor{context, plan.getCTEInfo(), plan.getPlanNode()};
    RemoveRedundantAggregateContext remove_context{context, {}};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, remove_context);
    plan.update(result);
    return true;
}

PlanNodePtr RemoveRedundantAggregateVisitor::visitPlanNode(PlanNodeBase & node, RemoveRedundantAggregateContext & ctx)
{
    //By default, the context is not passed to the parent node
    if (node.getChildren().empty())
        return node.shared_from_this();
    PlanNodes children;
    DataStreams inputs;
    for (const auto & item : node.getChildren())
    {
        RemoveRedundantAggregateContext child_context{ctx.context, {}};
        PlanNodePtr child = VisitorUtil::accept(*item, *this, child_context);
        children.emplace_back(child);
        inputs.push_back(child->getStep()->getOutputStream());
    }

    auto new_step = node.getStep()->copy(ctx.context);
    new_step->setInputStreams(inputs);
    node.setStep(new_step);

    node.replaceChildren(children);
    return node.shared_from_this();
}

PlanNodePtr RemoveRedundantAggregateVisitor::resetChildren(PlanNodeBase & node,PlanNodes & children, RemoveRedundantAggregateContext & ctx)
{
    DataStreams inputs;
    for(auto & child : children)
        inputs.push_back(child->getStep()->getOutputStream());
    auto new_step = node.getStep()->copy(ctx.context);
    new_step->setInputStreams(inputs);
    node.setStep(new_step);
    node.replaceChildren(children);
    return node.shared_from_this();
}
PlanNodePtr RemoveRedundantAggregateVisitor::visitLimitByNode(LimitByNode & node, RemoveRedundantAggregateContext & ctx)
{
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, ctx);
    PlanNodes children{child};
    return resetChildren(node, children, ctx);

}
PlanNodePtr RemoveRedundantAggregateVisitor::visitFilterNode(FilterNode & node, RemoveRedundantAggregateContext & ctx)
{
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, ctx);
    PlanNodes children{child};
    return resetChildren(node, children, ctx);
}
PlanNodePtr RemoveRedundantAggregateVisitor::visitDistinctNode(DistinctNode & node, RemoveRedundantAggregateContext & ctx)
{
    auto step = node.getStep();
    RemoveRedundantAggregateContext child_context{ctx.context, {}};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_context);

    //The distinct columns keys contains all the distinct keys of child nodes, remove distinct node.
    auto columns = step->getColumns();
    bool flag_distinct = false;
    for (auto & distinct_set : child_context.distincts)
    {
        flag_distinct = (flag_distinct || isDistinctNames(columns, distinct_set));
    }
    if (flag_distinct)
    {
        ctx.distincts = std::move(child_context.distincts);
        if (step->getLimitHint() == 0)
            return child;
    }
    //Equivalent group by, generate new distinct keys
    if (!columns.empty())
    {
        NameSet distinct_set;
        for (auto & column : columns)
            distinct_set.insert(column);
        ctx.distincts.emplace_back(distinct_set);
    }

    PlanNodes children{child};
    return resetChildren(node, children, ctx);
}
PlanNodePtr RemoveRedundantAggregateVisitor::visitAggregatingNode(AggregatingNode & node, RemoveRedundantAggregateContext & ctx)
{
    auto step = node.getStep();

    const AggregateDescriptions & descs = step->getAggregates();
    auto keys = step->getKeys();
    RemoveRedundantAggregateContext child_context{ctx.context, {}};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_context);
    bool flag_distinct = false;
    for (auto & distinct_set : child_context.distincts)
    {
        flag_distinct = (flag_distinct || isDistinctNames(keys, distinct_set));
    }
    //when distinct columns keys contains all the distinct keys of child nodes and func is empty, equivalent distinct node, remove
    if (flag_distinct && descs.empty() && !step->isPartial())
    {
        ctx.distincts = std::move(child_context.distincts);
        return child;
    }

    //need pass distinct keys to father node
    if (!keys.empty())
    {
        NameSet distinct_set;
        for (auto & key : keys)
            distinct_set.insert(key);
        ctx.distincts.emplace_back(distinct_set);
    }

    PlanNodes children{child};
    return resetChildren(node, children, ctx);
}
PlanNodePtr RemoveRedundantAggregateVisitor::visitProjectionNode(ProjectionNode & node, RemoveRedundantAggregateContext & ctx)
{
    auto step = node.getStep();

    //from child node get child distinct keys;
    RemoveRedundantAggregateContext child_context{ctx.context, {}};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_context);

    NameSet input_symbols;
    for (auto & column : step->getInputStreams()[0].header)
        input_symbols.insert(column.name);

    //filter assignment, just deal new assignment.
    for (auto & assignment : step->getAssignments())
    {
        if (input_symbols.find(assignment.first) == input_symbols.end())
        {
            //deal with the new assignment
            auto & ast = assignment.second;

            //only deal with ast is ASTIdentifier
            if(ast->getType() == ASTType::ASTIdentifier)
            {
                const auto & identifier = ast->as<ASTIdentifier &>();
                const String & symbol = identifier.name();
                //replace the every distinct alias with the assignment.first
                for (auto & distinct_set : child_context.distincts)
                {
                    if (distinct_set.count(symbol))
                    {
                        distinct_set.erase(symbol);
                        distinct_set.insert(assignment.first);
                    }
                }
            }
        }
    }
    //if every distinct alias is in output symbols, send the distinct_set to father node
    Names output_symbols;
    for (const auto & column : step->getOutputStream().header)
        output_symbols.emplace_back(column.name);
    for (auto & distinct_set : child_context.distincts)
    {
        if (isDistinctNames(output_symbols, distinct_set))
        {
            ctx.distincts.emplace_back(distinct_set);
        }
    }

    PlanNodes children{child};
    return resetChildren(node, children, ctx);
}

PlanNodePtr RemoveRedundantAggregateVisitor::visitCTERefNode(CTERefNode & node, RemoveRedundantAggregateContext & ctx)
{
    //in order to visit with {subquery} and remove redundant aggregate in subquery.
    auto with_step = node.getStep();

    RemoveRedundantAggregateContext child_context{ctx.context, {}};
    CTEId cte_id = with_step->getId();
    bool visited_flag = cte_helper.hasVisited(cte_id);
    auto cte_plan = cte_helper.acceptAndUpdate(cte_id, *this, child_context);

    //if cte not visited and distinct exist in child, insert the distinct to visit_results.
    if(!visited_flag && !child_context.distincts.empty())
    {
        visit_results.emplace(cte_id, child_context.distincts);
    }

    if(visit_results.count(cte_id))
    {
        std::vector<NameSet> distincts;
        // visit distincts record, according to output coulumns replace alias in distinct_set
        for(auto & distinct_set : visit_results[cte_id])
        {
            NameSet distinct_set_alias = distinct_set;
            for(const auto & item : with_step->getOutputColumns())
            {
                if(distinct_set.count(item.second))
                {
                    distinct_set_alias.erase(item.second);
                    distinct_set_alias.insert(item.first);
                }
            }
            distincts.emplace_back(distinct_set_alias);
        }
        ctx.distincts = std::move(distincts);
    }
//    context.distincts = std::move(child_context.distincts);

    DataStreams input_streams;
    input_streams.emplace_back(cte_plan->getStep()->getOutputStream());
    node.getStep()->setInputStreams(input_streams);
    return node.shared_from_this();
}

// Check every distinct column exist in names
bool RemoveRedundantAggregateVisitor::isDistinctNames(const Names & names, const NameSet & distinct_names)
{
    if (distinct_names.empty() || names.empty())
        return false;
    NameSet names_set{names.begin(), names.end()};
    for (const auto & distinct_alias : distinct_names)
    {
        if (!names_set.count(distinct_alias))
            return false;
    }
    return true;
}

PlanNodePtr RemoveRedundantAggregateVisitor::visitJoinNode(JoinNode & node, RemoveRedundantAggregateContext & ctx)
{
    auto step  = node.getStep();

    RemoveRedundantAggregateContext left_context{ctx.context, {}};
    auto left = VisitorUtil::accept(node.getChildren()[0], *this, left_context);
    RemoveRedundantAggregateContext right_context{ctx.context, {}};
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, right_context);

    const Names & left_names = step->getLeftKeys();
    const Names & right_names = step->getRightKeys();


    bool left_distinct_flag = false, right_distinct_flag = false;

    for (const auto & distinct_set : left_context.distincts)
        left_distinct_flag = (left_distinct_flag || isDistinctNames(left_names, distinct_set));

    for (const auto & distinct_set : right_context.distincts)
        right_distinct_flag = (right_distinct_flag || isDistinctNames(right_names, distinct_set));

    bool distinct_flag = left_distinct_flag && right_distinct_flag;
    ASTTableJoin::Kind kind = step->getKind();
    //left join need left_distinct_flag is true, and right_distinct_flag is true or strictness is any
    if (kind == ASTTableJoin::Kind::Left)
    {
        if (left_distinct_flag == true)
        {
            //step->getStrictness() == ASTTableJoin::Strictness::Semi
            if (step->getStrictness() == ASTTableJoin::Strictness::Any || right_distinct_flag == true)
                ctx.distincts = std::move(left_context.distincts);
        }
    }
    else if (kind == ASTTableJoin::Kind::Right)
    {
        if (right_distinct_flag == true)
        {
            //step->getStrictness() == ASTTableJoin::Strictness::Semi ||
            if (step->getStrictness() == ASTTableJoin::Strictness::Any || left_distinct_flag == true)
                ctx.distincts = std::move(right_context.distincts);
        }
    }
    //only left_distinct_flag and right_distinct_flag is true
    else if (kind == ASTTableJoin::Kind::Inner)
    {
        if (distinct_flag)
        {
            std::vector<NameSet> distincts_all;
            for (auto distinct_set : left_context.distincts)
                distincts_all.emplace_back(distinct_set);
            for (auto distinct_set : right_context.distincts)
                distincts_all.emplace_back(distinct_set);
            ctx.distincts = std::move(distincts_all);
        }
    }

    //reset input stream and children;
    PlanNodes children{left, right};
    return resetChildren(node, children, ctx);
}

PlanNodePtr RemoveRedundantAggregateVisitor::visitTableScanNode(TableScanNode & node, RemoveRedundantAggregateContext & ctx)
{
    const auto * step = dynamic_cast<const TableScanStep *>(node.getStep().get());
    auto storage = step->getStorage();

    auto meta = storage->getInMemoryMetadataPtr();
    if (!meta->getUniqueKey().definition_ast)
        return node.shared_from_this();

    if (meta->hasPartitionKey())
        return node.shared_from_this();

    std::vector<NameSet> distincts;
    NameSet distinct_set;
    NameSet distinct_alias;

    //from unique_ast get distinct column set
    auto symbols = extractSymbol(meta->getUniqueKey().definition_ast);

    for (const auto & symbol : symbols)
    {
        distinct_set.insert(symbol);
    }

    //get output alias set
    for (const auto & assigment : step->getColumnAlias())
    {
        if (distinct_set.count(assigment.first))
        {
            distinct_alias.insert(assigment.second);
        }
    }
    //when output alias include all distinct column, push to father context
    if (distinct_set.size() == distinct_alias.size())
    {
        distincts.emplace_back(distinct_alias);
        ctx.distincts = std::move(distincts);
    }

    return node.shared_from_this();
}

std::set<std::string> RemoveRedundantAggregateVisitor::extractSymbol(const ConstASTPtr & node)
{
    //when node is identifier, return column
    if(node->getType() == ASTType::ASTIdentifier)
    {
        const auto & identifier = node->as<ASTIdentifier &>();
        return {identifier.name()};
    }
    //else node is columns, so ast is func->expressionList->identifier
    //expressionList will exist this example [column_a, column_b, sipHash64(column_c)], so need to remove this example
    else if(node->getType() == ASTType::ASTFunction)
    {
        const auto & function = node->as<ASTFunction &>();
        if(function.name == "tuple")
        {
            const auto & child_node = function.children[0];
            if(child_node->getType() == ASTType::ASTExpressionList)
            {
                auto & expression_list = child_node->as<ASTExpressionList &>();
                std::set<std::string> symbols;
                for (const auto & child : expression_list.children)
                {
                    if(child->getType() == ASTType::ASTIdentifier)
                    {
                        const auto & identifier = child->as<ASTIdentifier &>();
                        symbols.emplace(identifier.name());
                    }
                    else
                    {
                        return {};
                    }
                }
                return symbols;
            }
        }
    }

    return {};
}
}
