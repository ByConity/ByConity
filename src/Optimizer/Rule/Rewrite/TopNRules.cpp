#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rewrite/TopNRules.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/SortingStep.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/TopNFilteringStep.h>

namespace DB
{

namespace
{
    UInt64 getMaxRowsToUseTopnFiltering(const ContextPtr & context)
    {
        UInt64 res = context->getSettingsRef().max_rows_to_use_topn_filtering;
        // better consider row count also
        if (res == 0)
            res = context->getSettingsRef().max_block_size / 10;
        return res;
    }
};

PatternPtr CreateTopNFilteringForAggregating::getPattern() const
{
    // TopN -> Aggregating -> !TopNFiltering
    // by this pattern, we also assume GROUP WITH TOTALS is not matched
    return Patterns::topN()
        .withSingle(Patterns::aggregating().withSingle(Patterns::any().matching(
            [](const PlanNodePtr & node, auto &) { return node->getStep()->getType() != IQueryPlanStep::Type::TopNFiltering; })))
        .result();
}

TransformResult CreateTopNFilteringForAggregating::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & topn_step = dynamic_cast<const SortingStep &>(*node->getStep());

    if (topn_step.getLimitValue() > getMaxRowsToUseTopnFiltering(context.context))
        return {};

    if (topn_step.hasPreparedParam())
        return {};
    auto & aggregate_node = node->getChildren()[0];
    const auto & aggregate_step = dynamic_cast<const AggregatingStep &>(*aggregate_node->getStep());

    NameSet agg_keys{aggregate_step.getKeys().begin(), aggregate_step.getKeys().end()};

    for (const auto & set_param : aggregate_step.getGroupingSetsParams())
    {
        for (auto it = agg_keys.begin(); it != agg_keys.end();)
        {
            if (std::find(set_param.used_key_names.begin(), set_param.used_key_names.end(), *it) == set_param.used_key_names.end())
                it = agg_keys.erase(it);
            else
                ++it;
        }
    }

    // if topN sort column is from an aggregate function, fail to rewrite
    for (const auto & sort_column_desc : topn_step.getSortDescription())
        if (!agg_keys.count(sort_column_desc.column_name))
            return {};

    const auto & aggregate_child_node = aggregate_node->getChildren()[0];

    auto topn_filter_step = std::make_shared<TopNFilteringStep>(
        aggregate_child_node->getStep()->getOutputStream(),
        topn_step.getSortDescription(),
        topn_step.getLimitValue(),
        TopNModel::DENSE_RANK);
    auto topn_filter_node = PlanNodeBase::createPlanNode(context.context->nextNodeId(), topn_filter_step, PlanNodes{aggregate_child_node});

    aggregate_node->replaceChildren(PlanNodes{topn_filter_node});
    return TransformResult{node};
}

PatternPtr PushTopNThroughProjection::getPattern() const
{
    return Patterns::topN().withSingle(Patterns::project()).result();
}

TransformResult PushTopNThroughProjection::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_ctx)
{
    auto projection = node->getChildren()[0];
    auto source = projection->getChildren()[0];

    const auto * topn_step = dynamic_cast<const SortingStep *>(node->getStep().get());
    const auto * project_step = dynamic_cast<const ProjectionStep *>(projection->getStep().get());

    if (!project_step || Utils::canChangeOutputRows(*project_step, rule_ctx.context))
        return {};

    auto symbols_exist_in_source = Utils::extractIdentities(*project_step);
    auto new_sort = topn_step->getSortDescription();

    for (auto & sort_column_desc : new_sort)
    {
        if (!symbols_exist_in_source.count(sort_column_desc.column_name))
            return {};
        sort_column_desc.column_name = symbols_exist_in_source.at(sort_column_desc.column_name);
    }

    auto new_topn = PlanNodeBase::createPlanNode(
        node->getId(),
        std::make_shared<SortingStep>(
            source->getStep()->getOutputStream(),
            new_sort,
            topn_step->getLimit(),
            topn_step->getStage(),
            topn_step->getPrefixDescription()),
        {source});

    projection->replaceChildren({new_topn});
    projection->setStatistics(node->getStatistics());
    return TransformResult{projection};
}

PatternPtr PushTopNFilteringThroughProjection::getPattern() const
{
    return Patterns::topNFiltering().withSingle(Patterns::project()).result();
}

TransformResult PushTopNFilteringThroughProjection::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    auto projection = node->getChildren()[0];
    auto source = projection->getChildren()[0];

    const auto * topn_filter_step = dynamic_cast<const TopNFilteringStep *>(node->getStep().get());
    const auto * project_step = dynamic_cast<const ProjectionStep *>(projection->getStep().get());

    auto symbols_exist_in_source = Utils::extractIdentities(*project_step);
    auto new_sort = topn_filter_step->getSortDescription();

    for (auto & sort_column_desc : new_sort)
    {
        if (!symbols_exist_in_source.count(sort_column_desc.column_name))
            return {};
        sort_column_desc.column_name = symbols_exist_in_source.at(sort_column_desc.column_name);
    }

    auto new_topn_filter = PlanNodeBase::createPlanNode(
        node->getId(),
        std::make_shared<TopNFilteringStep>(
            source->getStep()->getOutputStream(), new_sort, topn_filter_step->getSize(), topn_filter_step->getModel()),
        {source});

    projection->replaceChildren({new_topn_filter});
    projection->setStatistics(node->getStatistics());
    return TransformResult{projection};
}

PatternPtr PushTopNFilteringThroughUnion::getPattern() const
{
    return Patterns::topNFiltering().withSingle(Patterns::unionn()).result();
}

TransformResult PushTopNFilteringThroughUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & topn_filter_step = dynamic_cast<const TopNFilteringStep &>(*node->getStep());
    auto unionn = node->getChildren()[0];
    auto & unionn_step = dynamic_cast<UnionStep &>(*unionn->getStep());
    PlanNodes new_unionn_children;

    for (size_t idx = 0; idx < unionn->getChildren().size(); ++idx)
    {
        auto mappings = unionn_step.getOutToInput(idx);
        auto symbol_mapper = SymbolMapper::simpleMapper(mappings);
        auto source = unionn->getChildren().at(idx);

        auto new_source = PlanNodeBase::createPlanNode(
            context.context->nextNodeId(),
            std::make_shared<TopNFilteringStep>(
                source->getStep()->getOutputStream(),
                symbol_mapper.map(topn_filter_step.getSortDescription()),
                topn_filter_step.getSize(),
                topn_filter_step.getModel()),
            {source});

        new_unionn_children.push_back(new_source);
    }

    unionn->replaceChildren(new_unionn_children);
    return TransformResult{unionn};
}
}
