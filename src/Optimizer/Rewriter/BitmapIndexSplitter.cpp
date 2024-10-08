#include <memory>
#include <Optimizer/Rewriter/BitmapIndexSplitter.h>

#include <Optimizer/ExpressionRewriter.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/QueryPlan.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndex.h>


namespace DB
{
bool DB::BitmapIndexSplitter::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    FuncSplitter visitor{context};
    SplitterContext require;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, require);
    plan.update(result);
    return true;
}

PlanNodePtr createProjection(PlanNodes children, Assignments array_set_check_func, ContextMutablePtr & context)
{
    PlanNodePtr child = children[0];
    Assignments assignments;
    NameToType name_to_type;
    for (const auto & item : child->getCurrentDataStream().header)
    {
        assignments.emplace_back(item.name, std::make_shared<ASTIdentifier>(item.name));
        name_to_type[item.name] = item.type;
    }
    assignments.insert_back(array_set_check_func.begin(), array_set_check_func.end());

    ConstASTMap expr_map;
    for (const auto & item : array_set_check_func)
    {
        name_to_type[item.first] = std::make_shared<DataTypeUInt8>();
        expr_map[item.second] = ConstHashAST::make(std::make_shared<ASTIdentifier>(item.first));
    }


    auto projection = std::make_shared<ProjectionStep>(child->getCurrentDataStream(), assignments, name_to_type, false, true);
    LOG_DEBUG(
        getLogger("createProjection"),
        fmt::format(
            "projection input: {}, output: {}",
            projection->getInputStreams()[0].header.dumpStructure(),
            projection->getOutputStream().header.dumpStructure()));

    return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(projection), children);
}

PlanNodePtr FuncSplitter::visitPlanNode(PlanNodeBase & node, SplitterContext & splitter_context)
{
    if (node.getChildren().empty())
        return node.shared_from_this();

    PlanNodes children;
    for (size_t i = 0; i < node.getChildren().size(); i++)
    {
        const auto & child = node.getChildren()[i];
        if (i >= 1)
        {
            splitter_context.assignments.clear();
            splitter_context.ast_to_name.clear();
        }
        PlanNodePtr new_child = VisitorUtil::accept(*child, *this, splitter_context);
        children.emplace_back(new_child);
    }
    if (node.getChildren().size() > 1)
    {
        splitter_context.assignments.clear();
        splitter_context.ast_to_name.clear();
    }

    auto new_step = node.getStep()->copy(context);
    node.setStep(new_step);
    node.replaceChildren(children);
    return node.shared_from_this();
}

PlanNodePtr FuncSplitter::visitFilterNode(FilterNode & node, SplitterContext & splitter_context)
{
    auto new_node = visitPlanNode(node, splitter_context);
    if (const auto * filter_step = dynamic_cast<const FilterStep *>(new_node->getStep().get()))
    {
        auto result = CollectFuncs::collect(filter_step->getFilter(), new_node->getChildren()[0]->getCurrentDataStream().getNamesToTypes(), context);
        Assignments array_set_check_func;
        for (auto & item : result)
        {
            if (!splitter_context.ast_to_name.contains(item.second))
            {
                array_set_check_func.emplace_back(item);
                splitter_context.assignments.emplace_back(item);
                splitter_context.ast_to_name[item.second] = item.first;
            }
        }

        if (!array_set_check_func.empty())
        {
            PlanNodePtr projection_node = createProjection(new_node->getChildren(), array_set_check_func, context);

            PlanNodes children{projection_node};
            ConstASTMap expr_map;
            for (const auto & item : splitter_context.assignments)
            {
                expr_map[item.second] = ConstHashAST::make(std::make_shared<ASTIdentifier>(item.first));
            }
            auto filter = ExpressionRewriter::rewrite(filter_step->getFilter(), expr_map);
            auto new_filter = std::make_shared<FilterStep>(projection_node->getCurrentDataStream(), filter);

            return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(new_filter), {projection_node});
        }
    }
    return new_node;
}
PlanNodePtr FuncSplitter::visitProjectionNode(ProjectionNode & node, SplitterContext & splitter_context)
{
    auto new_node = visitPlanNode(node, splitter_context);

    if (const auto * projection_step = dynamic_cast<const ProjectionStep *>(new_node->getStep().get()))
    {
        if (projection_step->isFinalProject())
        {
            return new_node;
        }
        // split arraysetcheck func in projection
        Assignments array_set_check_func;
        auto tname_to_type = new_node->getChildren()[0]->getCurrentDataStream().getNamesToTypes();
        for (const auto & item : projection_step->getAssignments())
        {
            auto tmp_array_set_check_func = CollectFuncs::collect(item.second, tname_to_type, context);
            for (const auto & it : tmp_array_set_check_func)
            {
                if (!splitter_context.ast_to_name.contains(it.second))
                {
                    array_set_check_func.emplace_back(it);
                    splitter_context.assignments.emplace_back(it);
                    splitter_context.ast_to_name[it.second] = it.first;
                }
            }
        }

        PlanNodePtr bitmap_index_proj = new_node->getChildren()[0];
        if (!array_set_check_func.empty())
        {
            bitmap_index_proj = createProjection(new_node->getChildren(), array_set_check_func, context);
        }

        array_set_check_func = splitter_context.assignments;

        // replace funcs to identifier
        if (!array_set_check_func.empty())
        {
            ConstASTMap expr_map;
            for (const auto & item : splitter_context.assignments)
            {
                expr_map[item.second] = ConstHashAST::make(std::make_shared<ASTIdentifier>(item.first));
            }

            Assignments assignments;
            NameToType name_to_type;
            for (const auto & item : projection_step->getAssignments())
            {
                assignments.emplace_back(item.first, ExpressionRewriter::rewrite(item.second, expr_map));
                name_to_type[item.first] = projection_step->getNameToType().at(item.first);
            }

            for (const auto & item : bitmap_index_proj->getCurrentDataStream().header)
                if (!name_to_type.contains(item.name))
                {
                    assignments.emplace_back(item.name, std::make_shared<ASTIdentifier>(item.name));
                    name_to_type[item.name] = item.type;
                }

            auto projection
                = std::make_shared<ProjectionStep>(bitmap_index_proj->getCurrentDataStream(), assignments, name_to_type, false, true);

            return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(projection), {bitmap_index_proj});
        }
    }
    return new_node;
}

ConstASTPtr CollectFuncs::visitASTFunction(const ConstASTPtr & node, Assignments & assignments)
{
    visitNode(node, assignments);

    if (const auto * func = node->as<ASTFunction>())
    {
        if (functionCanUseBitmapIndex(*func) && func->arguments->children.size() > 0)
        {
            String argu_name = func->arguments->children[0]->getColumnName();
            if (name_to_type.contains(argu_name) && MergeTreeBitmapIndex::isBitmapIndexColumn(*name_to_type.at(argu_name)))
            {
                assignments.emplace_back(context->getSymbolAllocator()->newSymbol(node->getColumnName()), node->clone());
            }
        }
    }
    return node;
}
}
