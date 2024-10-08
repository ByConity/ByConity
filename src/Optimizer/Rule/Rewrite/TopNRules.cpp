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

    // create a TopNFiltering node below cur_node, return true if the creation is successful
    bool createTopNFilteringForAggLike(
        PlanNodePtr cur_node, const NameSet & agg_keys, SortDescription sort_desc, UInt64 limit_value, ContextMutablePtr context)
    {
        // if topN sort column is from an aggregate function, fail to rewrite
        for (const auto & sort_column_desc : sort_desc)
            if (!agg_keys.count(sort_column_desc.column_name))
                return false;

        // add other keys to sort keys to filter more data
        for (const auto & agg_key : agg_keys)
        {
            if (std::find_if(
                    sort_desc.begin(),
                    sort_desc.end(),
                    [&](const auto & sort_column_desc) { return sort_column_desc.column_name == agg_key; })
                == sort_desc.end())
                sort_desc.emplace_back(agg_key, 1, 1);
        }

        if (sort_desc.empty())
            return false;

        const auto & child_node = cur_node->getChildren()[0];

        auto topn_filter_step
            = std::make_shared<TopNFilteringStep>(child_node->getStep()->getOutputStream(), sort_desc, limit_value, TopNModel::DENSE_RANK);
        auto topn_filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), topn_filter_step, PlanNodes{child_node});

        cur_node->replaceChildren(PlanNodes{topn_filter_node});
        return true;
    }
};

ConstRefPatternPtr CreateTopNFilteringForAggregating::getPattern() const
{
    // TopN -> Aggregating -> !TopNFiltering
    // by this pattern, we also assume GROUP WITH TOTALS is not matched
    static auto pattern = Patterns::topN()
        .withSingle(Patterns::aggregating().withSingle(Patterns::any().matching(
            [](const QueryPlanStepPtr & step, auto &) { return step->getType() != IQueryPlanStep::Type::TopNFiltering; })))
        .result();
    return pattern;
}

TransformResult CreateTopNFilteringForAggregating::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & topn_step = dynamic_cast<const SortingStep &>(*node->getStep());
    if (topn_step.hasPreparedParam())
        return {};

    if (topn_step.getLimitValue() > getMaxRowsToUseTopnFiltering(context.context))
        return {};

    auto & agg_like_node = node->getChildren()[0];
    const auto & agg_like_step = dynamic_cast<const AggregatingStep &>(*agg_like_node->getStep());

    NameSet agg_keys{agg_like_step.getKeys().begin(), agg_like_step.getKeys().end()};

    // for grouping sets, use the intersection of keys
    for (const auto & set_param : agg_like_step.getGroupingSetsParams())
    {
        for (auto it = agg_keys.begin(); it != agg_keys.end();)
        {
            if (std::find(set_param.used_key_names.begin(), set_param.used_key_names.end(), *it) == set_param.used_key_names.end())
                it = agg_keys.erase(it);
            else
                ++it;
        }
    }

    if (!createTopNFilteringForAggLike(agg_like_node, agg_keys, topn_step.getSortDescription(), topn_step.getLimitValue(), context.context))
        return {};

    return TransformResult{node};
}

ConstRefPatternPtr CreateTopNFilteringForDistinct::getPattern() const
{
    static auto pattern = Patterns::topN()
        .withSingle(Patterns::distinct().withSingle(Patterns::any().matching(
            [](const QueryPlanStepPtr & step, auto &) { return step->getType() != IQueryPlanStep::Type::TopNFiltering; })))
        .result();
    return pattern;
}

TransformResult CreateTopNFilteringForDistinct::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & topn_step = dynamic_cast<const SortingStep &>(*node->getStep());
    if (topn_step.hasPreparedParam())
        return {};

    if (topn_step.getLimitValue() > getMaxRowsToUseTopnFiltering(context.context))
        return {};

    auto & agg_like_node = node->getChildren()[0];
    const auto & agg_like_step = dynamic_cast<const DistinctStep &>(*agg_like_node->getStep());

    NameSet agg_keys{agg_like_step.getColumns().begin(), agg_like_step.getColumns().end()};

    if (!createTopNFilteringForAggLike(agg_like_node, agg_keys, topn_step.getSortDescription(), topn_step.getLimitValue(), context.context))
        return {};

    return TransformResult{node};
}

ConstRefPatternPtr CreateTopNFilteringForAggregatingLimit::getPattern() const
{
    // TopN -> Aggregating -> !TopNFiltering
    // by this pattern, we also assume GROUP WITH TOTALS is not matched
    static auto pattern = Patterns::limit()
        .withSingle(Patterns::aggregating().withSingle(Patterns::any().matching(
            [](const QueryPlanStepPtr & step, auto &) { return step->getType() != IQueryPlanStep::Type::TopNFiltering; })))
        .result();
    return pattern;
}

TransformResult CreateTopNFilteringForAggregatingLimit::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & limit_step = dynamic_cast<const LimitStep &>(*node->getStep());
    if (limit_step.hasPreparedParam())
        return {};

    if (limit_step.getLimitValue() > getMaxRowsToUseTopnFiltering(context.context))
        return {};

    auto & agg_like_node = node->getChildren()[0];
    const auto & agg_like_step = dynamic_cast<const AggregatingStep &>(*agg_like_node->getStep());

    NameSet agg_keys{agg_like_step.getKeys().begin(), agg_like_step.getKeys().end()};

    // for grouping sets, use the intersection of keys
    for (const auto & set_param : agg_like_step.getGroupingSetsParams())
    {
        for (auto it = agg_keys.begin(); it != agg_keys.end();)
        {
            if (std::find(set_param.used_key_names.begin(), set_param.used_key_names.end(), *it) == set_param.used_key_names.end())
                it = agg_keys.erase(it);
            else
                ++it;
        }
    }

    if (!createTopNFilteringForAggLike(agg_like_node, agg_keys, SortDescription(), limit_step.getLimitValue(), context.context))
        return {};

    return TransformResult{node};
}

ConstRefPatternPtr CreateTopNFilteringForDistinctLimit::getPattern() const
{
    static auto pattern = Patterns::limit()
        .withSingle(Patterns::distinct().withSingle(Patterns::any().matching(
            [](const QueryPlanStepPtr & step, auto &) { return step->getType() != IQueryPlanStep::Type::TopNFiltering; })))
        .result();
    return pattern;
}

TransformResult CreateTopNFilteringForDistinctLimit::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & limit_step = dynamic_cast<const LimitStep &>(*node->getStep());
    if (limit_step.hasPreparedParam())
        return {};

    if (limit_step.getLimitValue() > getMaxRowsToUseTopnFiltering(context.context))
        return {};

    auto & agg_like_node = node->getChildren()[0];
    const auto & agg_like_step = dynamic_cast<const DistinctStep &>(*agg_like_node->getStep());

    NameSet agg_keys{agg_like_step.getColumns().begin(), agg_like_step.getColumns().end()};

    if (!createTopNFilteringForAggLike(agg_like_node, agg_keys, SortDescription(), limit_step.getLimitValue(), context.context))
        return {};

    return TransformResult{node};
}

ConstRefPatternPtr PushTopNThroughProjection::getPattern() const
{
    static auto pattern = Patterns::topN().withSingle(Patterns::project()).result();
    return pattern;
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

ConstRefPatternPtr PushTopNFilteringThroughProjection::getPattern() const
{
    static auto pattern = Patterns::topNFiltering().withSingle(Patterns::project()).result();
    return pattern;
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

ConstRefPatternPtr PushTopNFilteringThroughUnion::getPattern() const
{
    static auto pattern = Patterns::topNFiltering().withSingle(Patterns::unionn()).result();
    return pattern;
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
