/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <utility>
#include <Optimizer/Rule/Rewrite/PushPartialStepThroughExchangeRules.h>

#include <Core/SortDescription.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/PlanNodeCardinality.h>
#include <Optimizer/Rule/Pattern.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/SymbolUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/Hints/PushPartialAgg.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/SortingStep.h>
#include <QueryPlan/SymbolMapper.h>
#include <Poco/String.h>
#include <Poco/StringTokenizer.h>
#include <QueryPlan/UnionStep.h>

namespace DB
{

NameSet PushPartialAggThroughExchange::BLOCK_AGGS{
    "pathcount",
    "attributionanalysis",
    "attributioncorrelationfuse",
    "attribution",
    "attributioncorrelation",
    "bitmapjoinandcard",
    "bitmapjoinandcard2",
    "bitmapjoin",
    "bitmapcount",
    "bitmapextract",
    "bitmapmulticount",
    "bitmapmulticountwithdate",
    "bitmapmaxlevel",
    "bitmapcolumndiff"};

static std::pair<bool, bool> canPushPartialWithHint(const AggregatingStep * step)
{
    const auto & hint_list = step->getHints();
    for (const auto & hint : hint_list)
    {
        if (hint->getType() == HintCategory::PUSH_PARTIAL_AGG)
        {
            if (auto enable_hint = std::dynamic_pointer_cast<EnablePushPartialAgg>(hint))
                return {true, true};
            else if (auto disable_hint = std::dynamic_pointer_cast<DisablePushPartialAgg>(hint))
                return {true, false};
        }
    }
    return {false, true};
}

ConstRefPatternPtr PushPartialAggThroughExchange::getPattern() const
{
    static auto pattern = Patterns::aggregating().withSingle(Patterns::exchange()).result();
    return pattern;
}

TransformResult split(const PlanNodePtr & node, RuleContext & context)
{
    const auto * step = dynamic_cast<const AggregatingStep *>(node->getStep().get());
    QueryPlanStepPtr partial_agg = std::make_shared<AggregatingStep>(
        node->getChildren()[0]->getStep()->getOutputStream(),
        step->getKeys(),
        step->getKeysNotHashed(),
        step->getAggregates(),
        step->getGroupingSetsParams(),
        false,
        step->getGroupBySortDescription(),
        step->getGroupings(),
        step->needOverflowRow(),
        false,
        step->isNoShuffle(),
        step->isStreamingForCache(),
        step->getHints());

    auto partial_agg_node
        = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(partial_agg), node->getChildren(), node->getStatistics());

    Names keys;
    if (!step->getGroupingSetsParams().empty())
        keys.push_back("__grouping_set");
    keys.insert(keys.end(), step->getKeys().begin(), step->getKeys().end());

    ColumnNumbers keys_positions;
    auto exchange_header = partial_agg_node->getStep()->getOutputStream().header;

    for (const auto & key : keys)
    {
        keys_positions.emplace_back(exchange_header.getPositionByName(key));
    }

    Aggregator::Params new_params(
        exchange_header,
        keys_positions,
        step->getAggregates(),
        step->getParams().overflow_row,
        context.context->getSettingsRef().max_threads);

    auto transform_params = std::make_shared<AggregatingTransformParams>(new_params, step->isFinal());
    QueryPlanStepPtr final_agg = std::make_shared<MergingAggregatedStep>(
        partial_agg_node->getStep()->getOutputStream(),
        std::move(keys),
        step->getGroupingSetsParams(),
        step->getGroupings(),
        transform_params,
        false,
        context.context->getSettingsRef().max_threads,
        context.context->getSettingsRef().aggregation_memory_efficient_merge_threads);
    auto final_agg_node
        = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(final_agg), {partial_agg_node}, node->getStatistics());
    return final_agg_node;
}

PlanNodePtr createPartial(const AggregatingStep * step, PlanNodePtr child, NameToNameMap & map, Context & context)
{
    auto symbol_mapper = SymbolMapper::simpleMapper(map);
    auto agg_step = symbol_mapper.map(*step);
    auto mapped_partial = PlanNodeBase::createPlanNode(context.nextNodeId(), agg_step, {std::move(child)});

    if (map.empty())
    {
        return mapped_partial;
    }

    Assignments assignments;
    NameToType name_to_type;
    bool is_identity = true;
    for (const auto & output : agg_step->getOutputStream().header)
    {
        auto input = symbol_mapper.map(output.name);
        assignments.emplace_back(output.name, std::make_shared<ASTIdentifier>(input));
        is_identity &= output.name == input;
        name_to_type[output.name] = output.type;
    }

    if (is_identity)
    {
        return mapped_partial;
    }
    return PlanNodeBase::createPlanNode(
        context.nextNodeId(), std::make_shared<ProjectionStep>(agg_step->getOutputStream(), assignments, name_to_type), {mapped_partial});
}

TransformResult pushPartial(const PlanNodePtr & node, RuleContext & context)
{
    const auto * step = dynamic_cast<const AggregatingStep *>(node->getStep().get());
    auto exchange_node = node->getChildren()[0];
    const auto * exchange_step = dynamic_cast<const ExchangeStep *>(exchange_node->getStep().get());

    PlanNodes partials;
    for (size_t index = 0; index < exchange_step->getInputStreams().size(); ++index)
    {
        NameToNameMap map;
        for (const auto & item : exchange_step->getOutToInputs())
        {
            if (item.first != item.second[index])
            {
                map[item.first] = item.second[index];
            }
        }

        auto projection = createPartial(step, exchange_node->getChildren()[index], map, *context.context);
        partials.emplace_back(projection);
    }

    DataStreams exchange_inputs;
    for (const auto & item : partials)
    {
        exchange_inputs.emplace_back(item->getCurrentDataStream());
    }

    return PlanNodeBase::createPlanNode(
        context.context->nextNodeId(),
        std::make_shared<ExchangeStep>(
            exchange_inputs, exchange_step->getExchangeMode(), exchange_step->getSchema(), exchange_step->needKeepOrder()),
        partials);
}

TransformResult PushPartialAggThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const AggregatingStep *>(node->getStep().get());
    auto [has_push_partial_hint, enable_push_partical_agg] = canPushPartialWithHint(step);
    if (has_push_partial_hint)
    {
        if (!enable_push_partical_agg)
            return {};
    }
    else if (!context.context->getSettingsRef().enable_push_partial_agg && !step->isGroupingSet())
        return {};

    for (const auto & agg : step->getAggregates())
    {
        if (BLOCK_AGGS.count(Poco::toLower(agg.function->getName())))
        {
            return {};
        }
    }

    if (!context.context->getSettingsRef().enable_push_partial_block_list.value.empty())
    {
        Poco::StringTokenizer tokenizer(context.context->getSettingsRef().enable_push_partial_block_list, ",");
        NameSet block_names;
        for (const auto & name : tokenizer)
        {
            block_names.emplace(name);
        }

        for (const auto & agg : step->getAggregates())
        {
            if (block_names.count(agg.function->getName()))
            {
                return {};
            }
        }
    }

    if (step->isFinal())
        return split(node, context);
    else
        return pushPartial(node, context);
}

ConstRefPatternPtr PushPartialAggThroughUnion::getPattern() const
{
    static auto pattern = Patterns::aggregating()
        .matchingStep<AggregatingStep>([](const AggregatingStep & step) { return step.isPartial(); })
        .withSingle(Patterns::unionn()).result();
    return pattern;
}

TransformResult PushPartialAggThroughUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const AggregatingStep *>(node->getStep().get());
    auto union_node = node->getChildren()[0];
    auto [has_push_partial_hint, enable_push_partical_agg] = canPushPartialWithHint(step);
    if (has_push_partial_hint && !enable_push_partical_agg)
        return {};

    const auto * union_step = dynamic_cast<const UnionStep *>(union_node->getStep().get());

    PlanNodes partials;
    DataStreams union_inputs;
    for (size_t index = 0; index < union_step->getInputStreams().size(); ++index)
    {
        NameToNameMap map;
        for (const auto & item : union_step->getOutToInputs())
        {
            if (item.first != item.second[index])
            {
                map[item.first] = item.second[index];
            }
        }

        auto projection = createPartial(step, union_node->getChildren()[index], map, *context.context);

        partials.emplace_back(projection);
        union_inputs.emplace_back(projection->getCurrentDataStream());
    }

    NameToNameMap map;
    for (const auto & item : union_step->getOutToInputs())
    {
        map[item.second[0]] = item.first;
    }
    auto mapper = SymbolMapper::simpleMapper(map);

    DataStream output;
    for (const auto & item : union_inputs[0].header)
    {
        output.header.insert(ColumnWithTypeAndName{item.type, mapper.map(item.name)});
    }


    return PlanNodeBase::createPlanNode(
        context.context->nextNodeId(),
        std::make_shared<UnionStep>(union_inputs, output, OutputToInputs{}, union_step->getMaxThreads(), union_step->isLocal()),
        partials);
}

ConstRefPatternPtr PushPartialSortingThroughExchange::getPattern() const
{
    static auto pattern
        = Patterns::sorting()
              .matchingStep<SortingStep>([](const SortingStep & step) { return step.getStage() == SortingStep::Stage::FULL; })
              .withSingle(Patterns::exchange().matchingStep<ExchangeStep>(
                  [](const ExchangeStep & step) { return step.getExchangeMode() == ExchangeMode::GATHER; }))
              .result();
    return pattern;
}

TransformResult PushPartialSortingThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const SortingStep *>(node->getStep().get());
    auto old_exchange_node = node->getChildren()[0];
    const auto * old_exchange_step = dynamic_cast<const ExchangeStep *>(old_exchange_node->getStep().get());

    PlanNodes exchange_children;
    for (size_t index = 0; index < old_exchange_node->getChildren().size(); index++)
    {
        auto exchange_child = old_exchange_node->getChildren()[index];
        if (dynamic_cast<SortingNode *>(exchange_child.get()))
        {
            return {};
        }

        SortDescription new_sort_desc;
        for (const auto & desc : step->getSortDescription())
        {
            auto new_desc = desc;
            const auto & out_to_inputs = old_exchange_step->getOutToInputs();
            if (!out_to_inputs.contains(desc.column_name) || out_to_inputs.at(desc.column_name).size() <= index)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "PushPartialSortingThroughExchange: Can not find {} in out_to_inputs.", desc.column_name);
            new_desc.column_name = old_exchange_step->getOutToInputs().at(desc.column_name).at(index);
            new_sort_desc.emplace_back(new_desc);
        }

        auto before_exchange_sort = std::make_unique<SortingStep>(
            exchange_child->getStep()->getOutputStream(), new_sort_desc, step->getLimit(), SortingStep::Stage::PARTIAL, SortDescription{});
        PlanNodes children{exchange_child};
        auto before_exchange_sort_node
            = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(before_exchange_sort), children, node->getStatistics());
        exchange_children.emplace_back(before_exchange_sort_node);
    }

    auto exchange_step = old_exchange_step->copy(context.context);
    dynamic_cast<ExchangeStep *>(exchange_step.get())->setKeepOrder(true);
    auto exchange_node = PlanNodeBase::createPlanNode(
        context.context->nextNodeId(), std::move(exchange_step), exchange_children, old_exchange_node->getStatistics());

    QueryPlanStepPtr final_sort = step->copy(context.context);
    dynamic_cast<SortingStep *>(final_sort.get())->setStage(SortingStep::Stage::MERGE);
    PlanNodes exchange{exchange_node};
    auto final_sort_node
        = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(final_sort), exchange, node->getStatistics());
    return final_sort_node;
}

ConstRefPatternPtr PushPartialSortingThroughUnion::getPattern() const
{
    static auto pattern
        = Patterns::sorting()
              .matchingStep<SortingStep>([](const SortingStep & step) { return step.getStage() == SortingStep::Stage::PARTIAL; })
              .withSingle(Patterns::unionn())
              .result();
    return pattern;
}

TransformResult PushPartialSortingThroughUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const SortingStep *>(node->getStep().get());
    auto union_node = node->getChildren()[0];
    const auto * union_step = dynamic_cast<const UnionStep *>(union_node->getStep().get());

    PlanNodes union_inputs;
    for (size_t index = 0; index < union_node->getChildren().size(); index++)
    {
        auto exchange_child = union_node->getChildren()[index];
        if (dynamic_cast<SortingNode *>(exchange_child.get()))
            return {};

        SortDescription new_sort_desc;
        for (const auto & desc : step->getSortDescription())
        {
            auto new_desc = desc;
            const auto & out_to_inputs = union_step->getOutToInputs();
            if (!out_to_inputs.contains(desc.column_name) || out_to_inputs.at(desc.column_name).size() <= index)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "PushPartialSortingThroughUnion: Can not find {} in out_to_inputs.", desc.column_name);
            new_desc.column_name = union_step->getOutToInputs().at(desc.column_name).at(index);
            new_sort_desc.emplace_back(new_desc);
        }

        auto partial_sorting = std::make_unique<SortingStep>(
            exchange_child->getStep()->getOutputStream(), new_sort_desc, step->getLimit(), SortingStep::Stage::PARTIAL_NO_MERGE, SortDescription{});
        PlanNodes children{exchange_child};
        auto before_exchange_sort_node
            = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(partial_sorting), children, node->getStatistics());
        union_inputs.emplace_back(before_exchange_sort_node);
    }

    auto merging_sorted = std::make_unique<SortingStep>(
        step->getOutputStream(), step->getSortDescription(), step->getLimit(), SortingStep::Stage::MERGE, SortDescription{});

    return PlanNodeBase::createPlanNode(
        context.context->nextNodeId(),
        std::move(merging_sorted),
        {PlanNodeBase::createPlanNode(context.context->nextNodeId(), union_node->getStep(), union_inputs)});
}

static bool isLimitNeeded(const LimitStep & limit, const PlanNodePtr & node)
{
    auto range = PlanNodeCardinality::extractCardinality(*node);
    return !limit.hasPreparedParam() && range.upperBound > limit.getLimitValue() + limit.getOffsetValue();
}

ConstRefPatternPtr PushPartialLimitThroughExchange::getPattern() const
{
    static auto pattern = Patterns::limit().withSingle(Patterns::exchange()).result();
    return pattern;
}

TransformResult PushPartialLimitThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const LimitStep *>(node->getStep().get());
    auto old_exchange_node = node->getChildren()[0];
    const auto * old_exchange_step = dynamic_cast<const ExchangeStep *>(old_exchange_node->getStep().get());

    PlanNodes exchange_children;
    bool should_apply = false;
    for (const auto & exchange_child : old_exchange_node->getChildren())
    {
        if (isLimitNeeded(*step, exchange_child))
        {
            auto partial_limit = std::make_unique<LimitStep>(
                exchange_child->getStep()->getOutputStream(),
                step->getLimitValue() + step->getOffsetValue(),
                size_t{0},
                false,
                false,
                step->getSortDescription(),
                true);
            auto partial_limit_node = PlanNodeBase::createPlanNode(
                context.context->nextNodeId(), std::move(partial_limit), PlanNodes{exchange_child}, node->getStatistics());
            exchange_children.emplace_back(partial_limit_node);

            should_apply = true;
        }
    }

    if (!should_apply)
        return {};

    auto exchange_step = old_exchange_step->copy(context.context);
    auto exchange_node = PlanNodeBase::createPlanNode(
        context.context->nextNodeId(), std::move(exchange_step), exchange_children, old_exchange_node->getStatistics());

    node->replaceChildren({exchange_node});
    return node;
}

ConstRefPatternPtr PushPartialDistinctThroughExchange::getPattern() const
{
    static auto pattern = Patterns::distinct()
        .matchingStep<DistinctStep>([](const DistinctStep & step) { return !step.preDistinct(); })
        .withSingle(Patterns::exchange()).result();
    return pattern;
}

TransformResult PushPartialDistinctThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const DistinctStep *>(node->getStep().get());
    auto old_exchange_node = node->getChildren()[0];
    const auto * old_exchange_step = dynamic_cast<const ExchangeStep *>(old_exchange_node->getStep().get());
    if (dynamic_cast<const DistinctStep *>(old_exchange_node->getChildren()[0]->getStep().get()))
    {
        return {};
    }

    PlanNodes exchange_children;
    for (const auto & exchange_child : old_exchange_node->getChildren())
    {
        auto partial_limit = std::make_unique<DistinctStep>(
            exchange_child->getStep()->getOutputStream(),
            step->getSetSizeLimits(),
            step->getLimitHint(),
            step->getColumns(),
            true,
            step->canToAgg());
        auto partial_limit_node = PlanNodeBase::createPlanNode(
            context.context->nextNodeId(), std::move(partial_limit), PlanNodes{exchange_child}, node->getStatistics());
        exchange_children.emplace_back(partial_limit_node);
    }
    auto exchange_step = old_exchange_step->copy(context.context);
    auto exchange_node = PlanNodeBase::createPlanNode(
        context.context->nextNodeId(), std::move(exchange_step), exchange_children, old_exchange_node->getStatistics());

    node->replaceChildren({exchange_node});
    return node;
}

ConstRefPatternPtr PushPartialTopNDistinctThroughExchange::getPattern() const
{
    static auto pattern = Patterns::limit()
        .withSingle(
            Patterns::sorting().withSingle(Patterns::distinct()
                                               .matchingStep<DistinctStep>([](const DistinctStep & step) { return !step.preDistinct(); })
                                               .withSingle(Patterns::exchange())))
        .result();
    return pattern;
}

TransformResult PushPartialTopNDistinctThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * limit_step = dynamic_cast<const LimitStep *>(node->getStep().get());
    const auto * sort_step = dynamic_cast<const SortingStep *>(node->getChildren()[0]->getStep().get());

    const auto * step = dynamic_cast<const DistinctStep *>(node->getChildren()[0]->getChildren()[0]->getStep().get());
    auto old_exchange_node = node->getChildren()[0]->getChildren()[0]->getChildren()[0];
    const auto * old_exchange_step = dynamic_cast<const ExchangeStep *>(old_exchange_node->getStep().get());
    PlanNodes sorting_children;
    for (const auto & exchange_child : old_exchange_node->getChildren())
    {
        auto partial_distinct = std::make_unique<DistinctStep>(
            exchange_child->getStep()->getOutputStream(), SizeLimits{}, 0, step->getColumns(), true, step->canToAgg());
        auto partial_distinct_node = PlanNodeBase::createPlanNode(
            context.context->nextNodeId(), std::move(partial_distinct), PlanNodes{exchange_child}, node->getStatistics());
        sorting_children.emplace_back(partial_distinct_node);
    }

    auto partial_sort = std::make_unique<SortingStep>(
        sorting_children[0]->getStep()->getOutputStream(),
        sort_step->getSortDescription(),
        sort_step->getLimit(),
        SortingStep::Stage::PARTIAL,
        SortDescription{});
    auto partial_sort_node
        = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(partial_sort), sorting_children, node->getStatistics());

    auto distinct_limit = step->copy(context.context);
    dynamic_cast<DistinctStep *>(distinct_limit.get())->setLimitHint(limit_step->getLimitValue() + limit_step->getOffsetValue());
    auto before_exchange = PlanNodeBase::createPlanNode(
        context.context->nextNodeId(),
        limit_step->copy(context.context),
        {PlanNodeBase::createPlanNode(context.context->nextNodeId(), distinct_limit, {partial_sort_node})});


    auto exchange_step = old_exchange_step->copy(context.context);
    dynamic_cast<ExchangeStep *>(exchange_step.get())->setKeepOrder(true);
    auto exchange_node = PlanNodeBase::createPlanNode(
        context.context->nextNodeId(), std::move(exchange_step), {before_exchange}, old_exchange_node->getStatistics());

    QueryPlanStepPtr final_sort = sort_step->copy(context.context);
    dynamic_cast<SortingStep *>(final_sort.get())->setStage(SortingStep::Stage::MERGE);
    PlanNodes exchange{exchange_node};
    auto final_sort_node
        = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(final_sort), exchange, node->getStatistics());


    return PlanNodeBase::createPlanNode(
        context.context->nextNodeId(),
        limit_step->copy(context.context),
        {PlanNodeBase::createPlanNode(context.context->nextNodeId(), step->copy(context.context), {final_sort_node})});
}

ConstRefPatternPtr MarkTopNDistinctThroughExchange::getPattern() const
{
    static auto pattern = Patterns::limit()
        .withSingle(Patterns::sorting().withSingle(
            Patterns::distinct().matchingStep<DistinctStep>([](const DistinctStep & step) { return !step.preDistinct() && step.canToAgg(); })))
        .result();
    return pattern;
}

TransformResult MarkTopNDistinctThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto distinct_node = node->getChildren()[0]->getChildren()[0];
    const auto * step = dynamic_cast<const DistinctStep *>(distinct_node->getStep().get());

    PlanNodes exchange_children;
    auto distinct_step = std::make_unique<DistinctStep>(
        distinct_node->getChildren()[0]->getCurrentDataStream(),
        step->getSetSizeLimits(),
        step->getLimitHint(),
        step->getColumns(),
        step->preDistinct(),
        false);
    auto new_distinct = PlanNodeBase::createPlanNode(
        context.context->nextNodeId(), std::move(distinct_step), distinct_node->getChildren(), node->getStatistics());

    node->getChildren()[0]->replaceChildren({new_distinct});
    return node;
}
}
