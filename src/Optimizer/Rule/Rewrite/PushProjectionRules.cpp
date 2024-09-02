#include <memory>
#include <Optimizer/Rule/Rewrite/PushProjectionRules.h>

#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/Rewriter/BitmapIndexSplitter.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/SymbolsExtractor.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/LimitStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/SymbolMapper.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>

namespace DB
{

ConstRefPatternPtr PushProjectionThroughFilter::getPattern() const
{
    static auto pattern = Patterns::project().withSingle(Patterns::filter()).result();
    return pattern;
}

TransformResult PushProjectionThroughFilter::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto * projection = dynamic_cast<const ProjectionStep *>(node->getStep().get());

    size_t func_count = 0;
    
    auto tname_to_type = node->getChildren()[0]->getCurrentDataStream().getNamesToTypes();
    for (const auto & item : projection->getAssignments())
    {
        func_count += CollectFuncs::collect(item.second, tname_to_type, rule_context.context).size();
    }

    if (!func_count)
        return {};

    auto output_names = projection->getOutputStream().header.getNameSet();
    auto output_header = node->getCurrentDataStream().header;
    auto input_header = projection->getInputStreams()[0].header;

    bool has_all = true;
    for (const auto & item : input_header)
    {
        if (!output_names.count(item.name))
        {
            has_all = false;
            break;
        }
    }

    if (has_all)
    {
        // before: ProjectionA -> Filter -> TableScan
        // after:  Filter -> ProjectionA -> TableScan
        auto filter_step = node->getChildren()[0]->getStep()->copy(rule_context.context);
        node->replaceChildren(node->getChildren()[0]->getChildren());
        filter_step->setInputStreams({node->getCurrentDataStream()});
        return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), filter_step, {node});
    }
    else
    {
        // before: ProjectionA -> Filter -> TableScan
        // after:  ProjectionB -> Filter -> ProjectionA -> TableScan

        auto bottom_projection_node = [&] {
            auto new_ass = projection->getAssignments();
            auto name_to_type = projection->getNameToType();
            // We should add cols used only by filter to assignments of ProjectionA
            //    to be used by Filter
            // Here we add all input cols into assignments.
            // Some cols are not used by filter and not in output,
            //   they will be trimmed by columnPruning later
            for (const auto & item : input_header)
            {
                if (name_to_type.count(item.name))
                    continue;
                new_ass.emplace_back(item.name, std::make_shared<ASTIdentifier>(item.name));
                name_to_type[item.name] = item.type;
            }
            auto step = std::make_shared<ProjectionStep>(projection->getInputStreams()[0], new_ass, name_to_type, projection->isFinalProject(), projection->isIndexProject());
            return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), step, node->getChildren()[0]->getChildren());
        }();


        auto filter_step = node->getChildren()[0]->getStep()->copy(rule_context.context);
        filter_step->setInputStreams({bottom_projection_node->getCurrentDataStream()});
        auto filter_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), filter_step, {bottom_projection_node});

        // construct ProjectionB, return cols only in output_header
        auto top_projection_node = [&] {
            Assignments new_ass;
            NameToType name_to_type;
            for (const auto & item : output_header)
            {
                new_ass.emplace_back(item.name, std::make_shared<ASTIdentifier>(item.name));
                name_to_type[item.name] = item.type;
            }
            auto step = std::make_shared<ProjectionStep>(filter_node->getCurrentDataStream(), new_ass, name_to_type);

            return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), step, {filter_node});
        }();
        return top_projection_node;
    }
}

ConstRefPatternPtr PushProjectionThroughProjection::getPattern() const
{
    static auto pattern = Patterns::project().withSingle(Patterns::project()).result();
    return pattern;
}

TransformResult PushProjectionThroughProjection::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto * projection = dynamic_cast<const ProjectionStep *>(node->getStep().get());
    auto * bottom_projection = dynamic_cast<const ProjectionStep *>(node->getChildren()[0]->getStep().get());

    auto tname_to_type = node->getChildren()[0]->getCurrentDataStream().getNamesToTypes();
    size_t func_count = 0;
    for (const auto & item : projection->getAssignments())
    {
        func_count += CollectFuncs::collect(item.second, tname_to_type, rule_context.context).size();
    }

    if (!func_count)
        return {};

    auto output_header = node->getCurrentDataStream().header;
    auto input_header = projection->getInputStreams()[0].header;

    // before: ProjectionA -> ProjectionB -> TableScan
    // after:  Rename Projection -> ProjectionB -> ProjectionA -> TableScan

    NameSet require_symbols;
    for (const auto & item : projection->getAssignments())
    {
        if (!item.second->as<ASTIdentifier>())
        {
            auto symbols = SymbolsExtractor::extract(item.second);
            require_symbols.insert(symbols.begin(), symbols.end());
        }
    }

    NameToNameMap transform_map;
    for (const auto & item : require_symbols)
    {
        const auto * id = bottom_projection->getAssignments().at(item)->as<ASTIdentifier>();
        if (!id)
            return {};
        if (id->name() != item)
            transform_map.emplace(item, id->name());
    }
    SymbolMapper mapper = SymbolMapper::simpleMapper(transform_map);

    auto bottom_projection_node = [&] {
        auto new_ass = projection->getAssignments();
        auto name_to_type = projection->getNameToType();
        for (const auto & item : input_header)
        {
            if (name_to_type.count(item.name))
                continue;
            new_ass.emplace_back(item.name, std::make_shared<ASTIdentifier>(item.name));
            name_to_type[item.name] = item.type;
        }

        for (const auto & item : bottom_projection->getInputStreams()[0].header)
        {
            if (!name_to_type.contains(item.name))
            {
                name_to_type[item.name] = item.type;
                new_ass.emplace_back(item.name, std::make_shared<ASTIdentifier>(item.name));
            }
        }
        auto step = std::make_shared<ProjectionStep>(bottom_projection->getInputStreams()[0], mapper.map(new_ass), mapper.map(name_to_type), projection->isFinalProject(), projection->isIndexProject());
        return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), step, node->getChildren()[0]->getChildren());
    }();

    auto middle_projection_node = [&] {
        Assignments new_ass;
        NameToType name_to_type;
        auto old_name_to_type = bottom_projection->getNameToType();
        auto old_ass = bottom_projection->getAssignments();
        for (const auto & item : output_header)
        {
            new_ass.emplace_back(
                item.name, old_name_to_type.count(item.name) ? old_ass.at(item.name) : std::make_shared<ASTIdentifier>(item.name));
            name_to_type[item.name] = item.type;
        }
        auto step = std::make_shared<ProjectionStep>(bottom_projection_node->getCurrentDataStream(), new_ass, name_to_type, bottom_projection->isFinalProject(), bottom_projection->isIndexProject());
        return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), step, {bottom_projection_node});
    }();

    return middle_projection_node;
}
}
