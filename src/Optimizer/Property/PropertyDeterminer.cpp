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

#include <set>
#include <Optimizer/Property/PropertyDeterminer.h>

#include <Optimizer/Property/Property.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/TotalsHavingStep.h>
#include <QueryPlan/UnionStep.h>
#include <QueryPlan/WindowStep.h>
#include <Storages/StorageCnchMergeTree.h>

namespace DB
{
PropertySets PropertyDeterminer::determineRequiredProperty(QueryPlanStepPtr step, const Property & property, Context & context)
{
    DeterminerContext ctx{property, context};
    DeterminerVisitor visitor{};
    PropertySets input_properties = VisitorUtil::accept(step, visitor, ctx);
    if (!property.getCTEDescriptions().empty() || !property.getTableLayout().empty())
    {
        for (auto & property_set : input_properties)
        {
            for (auto & prop : property_set)
            {
                prop.setCTEDescriptions(property.getCTEDescriptions());
                prop.setTableLayout(property.getTableLayout());
            }
        }
    }
    return input_properties;
}


PropertySets DeterminerVisitor::visitStep(const IQueryPlanStep &, DeterminerContext & context)
{
    return {{context.getRequired()}};
}

PropertySets DeterminerVisitor::visitMultiJoinStep(const MultiJoinStep & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

PropertySets DeterminerVisitor::visitBufferStep(const BufferStep & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

PropertySets DeterminerVisitor::visitPartitionTopNStep(const PartitionTopNStep &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

PropertySets DeterminerVisitor::visitLocalExchangeStep(const LocalExchangeStep & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

PropertySets DeterminerVisitor::visitOffsetStep(const OffsetStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitFinishSortingStep(const FinishSortingStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitFinalSampleStep(const FinalSampleStep & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

PropertySets DeterminerVisitor::visitProjectionStep(const ProjectionStep & step, DeterminerContext & ctx)
{
    if (step.isFinalProject() && (ctx.getRequired().getNodePartitioning().getComponent() != Partitioning::Component::WORKER))
        return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
    const auto & assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    auto translated = ctx.getRequired().translate(identities);
    if (!step.getInputStreams()[0].header)
        return {{Property{}}};
    translated.setPreferred(true);
    return {{translated}};
}

PropertySets DeterminerVisitor::visitArrayJoinStep(const ArrayJoinStep &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

PropertySets DeterminerVisitor::visitFilterStep(const FilterStep &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

// TODO property expand @jingpeng
PropertySets DeterminerVisitor::visitJoinStep(const JoinStep & step, DeterminerContext & context)
{
    const Names & left_keys = step.getLeftKeys();
    const Names & right_keys = step.getRightKeys();

    auto enforce_round_robine = context.getContext().getSettingsRef().enforce_round_robin;
    // process ASOF join, it is different with normal join.
    if (step.getStrictness() == ASTTableJoin::Strictness::Asof)
    {
        Names left_keys_asof;
        Names right_keys_asof;
        for (size_t i = 0; i < left_keys.size() - 1; ++i)
        {
            left_keys_asof.emplace_back(left_keys[i]);
            right_keys_asof.emplace_back(right_keys[i]);
        }

        Partitioning left_stream{Partitioning::Handle::FIXED_HASH, left_keys_asof};
        Partitioning right_stream{Partitioning::Handle::FIXED_HASH, right_keys_asof};

        Property left{Partitioning{Partitioning::Handle::FIXED_HASH, left_keys_asof, false, 0, nullptr, enforce_round_robine}, left_stream};
        Property right{Partitioning{Partitioning::Handle::FIXED_HASH, right_keys_asof, false, 0, nullptr, false}, right_stream};
        PropertySet set;
        set.emplace_back(left);
        set.emplace_back(right);
        return {set};
    }

    if (step.getDistributionType() == DistributionType::BROADCAST)
    {
        auto left_require = context.getRequired();
        left_require.setPreferred(true);
        return {{left_require, Property{Partitioning{Partitioning::Handle::FIXED_BROADCAST}}}};
    }

    if (left_keys.empty() && right_keys.empty())
    {
        Property left{Partitioning{Partitioning::Handle::SINGLE}};
        Property right{Partitioning{Partitioning::Handle::SINGLE}};
        PropertySet set;
        set.emplace_back(left);
        set.emplace_back(right);
        return {set};
    }

    std::vector<std::tuple<String, String>> join_key_pairs;
    for (size_t i = 0; i < left_keys.size(); ++i)
    {
        join_key_pairs.emplace_back(std::make_tuple(left_keys[i], right_keys[i]));
    }

    PropertySets result;
    if (join_key_pairs.size() <= context.getContext().getSettingsRef().max_expand_join_key_size)
    {
        for (auto & set : Utils::powerSet(join_key_pairs))
        {
            Names sub_left_keys;
            Names sub_right_keys;
            for (const auto & item : set)
            {
                sub_left_keys.emplace_back(std::get<0>(item));
                sub_right_keys.emplace_back(std::get<1>(item));
            }

            Partitioning left_stream{Partitioning::Handle::FIXED_HASH, sub_left_keys};
            Partitioning right_stream{Partitioning::Handle::FIXED_HASH, sub_right_keys};
            Property left{Partitioning{Partitioning::Handle::FIXED_HASH, sub_left_keys, false, 0, nullptr, enforce_round_robine}, left_stream};
            Property right{Partitioning{Partitioning::Handle::FIXED_HASH, sub_right_keys, false, 0, nullptr, false}, right_stream};
            PropertySet prop_set;
            prop_set.emplace_back(left);
            prop_set.emplace_back(right);
            result.emplace_back(prop_set);
        }
    }
    else
    {
        Partitioning left_stream{Partitioning::Handle::FIXED_HASH, left_keys};
        Partitioning right_stream{Partitioning::Handle::FIXED_HASH, right_keys};
        Property left{Partitioning{Partitioning::Handle::FIXED_HASH, left_keys, false, 0, nullptr, enforce_round_robine}, left_stream};
        Property right{Partitioning{Partitioning::Handle::FIXED_HASH, right_keys, false, 0, nullptr, false}, right_stream};
        PropertySet prop_set;
        prop_set.emplace_back(left);
        prop_set.emplace_back(right);
        result.emplace_back(prop_set);
    }
    return result;
}

PropertySets DeterminerVisitor::visitAggregatingStep(const AggregatingStep & step, DeterminerContext & context)
{
    if (!context.getContext().getSettingsRef().enable_shuffle_before_state_func && step.getAggregates().size() > 0)
    {
        bool all_state_agg = true;
        for (const auto & agg : step.getAggregates())
        {
            if (!agg.function->getName().ends_with("State"))
            {
                all_state_agg = false;
                break;
            }
        }
        if (all_state_agg)
        {
            auto require = context.getRequired();
            require.setPreferred(true);
            return {{require}};
        }
    }
    //    if (/*step.isTotals() || */)
    //    {
    //        return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
    //    }
    if (!step.isFinal())
    {
        auto require = context.getRequired();
        require.setPreferred(true);
        return {{require}};
    }

    auto keys = step.getKeys();
    if (keys.empty())
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{Partitioning::Handle::SINGLE}});
        return {set};
    }

    PropertySets sets;
    auto required_keys = context.getRequired().getNodePartitioning().getColumns();
    if (context.getContext().getSettingsRef().enable_merge_require_property && !required_keys.empty() && keys.size() > required_keys.size())
    {
        std::set<String> keys_set(keys.begin(), keys.end());
        bool contain_all = true;
        for (auto & required_key : required_keys)
        {
            if (!keys_set.contains(required_key))
            {
                contain_all = false;
                break;
            }
        }

        if (contain_all)
            sets.emplace_back(
                PropertySet{Property{context.getRequired().getNodePartitioning(), context.getRequired().getStreamPartitioning()}});
    }

    if (keys.size() <= context.getContext().getSettingsRef().max_expand_agg_key_size)
    {
        for (const auto & sub_keys : Utils::powerSet(keys))
        {
            Property prop{
                Partitioning{Partitioning::Handle::FIXED_HASH, sub_keys}, Partitioning{Partitioning::Handle::FIXED_HASH, sub_keys}};
            sets.emplace_back(PropertySet{prop});
        }
    }
    else
    {
        sets.emplace_back(PropertySet{
            Property{Partitioning{Partitioning::Handle::FIXED_HASH, keys}, Partitioning{Partitioning::Handle::FIXED_HASH, keys}}});
    }

    if (step.isGroupingSet())
    {
        keys.emplace_back("__grouping_set");
        return {PropertySet{
            Property{Partitioning{Partitioning::Handle::FIXED_HASH, keys, false, 0, nullptr, true, Partitioning::Component::ANY, true}}}};
    }

    return sets;
}

PropertySets DeterminerVisitor::visitTotalsHavingStep(const TotalsHavingStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitMarkDistinctStep(const MarkDistinctStep & step, DeterminerContext &)
{
    auto keys = step.getDistinctSymbols();
    if (keys.empty())
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{Partitioning::Handle::SINGLE}});
        return {set};
    }

    PropertySets sets;

    sets.emplace_back(PropertySet{Property{Partitioning{
        Partitioning::Handle::FIXED_HASH,
        keys,
    }}});

    return sets;
}

PropertySets DeterminerVisitor::visitMergingAggregatedStep(const MergingAggregatedStep & step, DeterminerContext &)
{
    auto keys = step.getKeys();
    if (keys.empty())
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{Partitioning::Handle::SINGLE}});
        return {set};
    }
    std::vector<String> group_bys;
    group_bys.reserve(keys.size());
    for (const auto & key : keys)
    {
        group_bys.emplace_back(key);
    }
    PropertySet set;
    set.emplace_back(Property{
        Partitioning{
            Partitioning::Handle::FIXED_HASH,
            group_bys,
        },
        Partitioning{
            Partitioning::Handle::FIXED_HASH,
            group_bys,
        }

    });
    return {set};
}

PropertySets DeterminerVisitor::visitUnionStep(const UnionStep & step, DeterminerContext & context)
{
    PropertySet set;
    for (size_t i = 0; i < step.getInputStreams().size(); ++i)
    {
        std::unordered_map<String, String> mapping;
        for (const auto & output_to_input : step.getOutToInputs())
        {
            mapping[output_to_input.first] = output_to_input.second[i];
        }
        Property translated = context.getRequired().translate(mapping);
        translated.setPreferred(true);
        set.emplace_back(translated);
    }
    return {set};
}

PropertySets DeterminerVisitor::visitIntersectStep(const IntersectStep & node, DeterminerContext &)
{
    PropertySet set;
    for (const auto & input : node.getInputStreams())
    {
        set.emplace_back(Property{Partitioning{
            Partitioning::Handle::FIXED_HASH,
            input.header.getNames(),
        }});
    }

    return {set};
}

PropertySets DeterminerVisitor::visitExceptStep(const ExceptStep & node, DeterminerContext & context)
{
    return visitStep(node, context);
}

PropertySets DeterminerVisitor::visitIntersectOrExceptStep(const IntersectOrExceptStep & node, DeterminerContext &)
{
    PropertySet set;
    for (const auto & input : node.getInputStreams())
    {
        set.emplace_back(Property{Partitioning{
            Partitioning::Handle::FIXED_HASH,
            input.header.getNames(),
        }});
    }

    return {set};
}

PropertySets DeterminerVisitor::visitExchangeStep(const ExchangeStep &, DeterminerContext &)
{
    return {{Property{}}};
}

PropertySets DeterminerVisitor::visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep & node, DeterminerContext & context)
{
    return visitStep(node, context);
}

PropertySets DeterminerVisitor::visitTableScanStep(const TableScanStep &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitReadNothingStep(const ReadNothingStep &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitReadStorageRowCountStep(const ReadStorageRowCountStep &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitValuesStep(const ValuesStep &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitLimitStep(const LimitStep & step, DeterminerContext & context)
{
    if (step.isPartial())
        return visitStep(step, context);
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitLimitByStep(const LimitByStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitSortingStep(const SortingStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}
PropertySets DeterminerVisitor::visitMergeSortingStep(const MergeSortingStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}
PropertySets DeterminerVisitor::visitPartialSortingStep(const PartialSortingStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}
PropertySets DeterminerVisitor::visitMergingSortedStep(const MergingSortedStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}
//PropertySets DeterminerVisitor::visitMaterializingStep(const MaterializingStep & node, DeterminerContext & context)
//{
//    return visitPlan(node, context);
//}
//
//PropertySets DeterminerVisitor::visitDecompressionStep(const DecompressionStep & node, DeterminerContext & context)
//{
//    return visitPlan(node, context);
//}

PropertySets DeterminerVisitor::visitDistinctStep(const DistinctStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitExtremesStep(const ExtremesStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}}};
}
//PropertySets DeterminerVisitor::visitFinalSamplingStep(const FinalSamplingStep &, DeterminerContext &)
//{
//    return {{Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}}};
//}

PropertySets DeterminerVisitor::visitWindowStep(const WindowStep & step, DeterminerContext & context)
{
    auto keys = step.getWindow().partition_by;
    if (keys.empty())
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{Partitioning::Handle::SINGLE}});
        return {set};
    }
    std::vector<String> group_bys;
    for (const auto & key : keys)
    {
        group_bys.emplace_back(key.column_name);
    }
    PropertySets sets;
    if (keys.size() <= context.getContext().getSettingsRef().max_expand_agg_key_size)
    {
        for (const auto & sub_keys : Utils::powerSet(group_bys))
        {
            Property prop{Partitioning{Partitioning::Handle::FIXED_HASH, sub_keys}};
            sets.emplace_back(PropertySet{prop});
        }
    }
    else
    {
        PropertySet set;
        set.emplace_back(Property{Partitioning{Partitioning::Handle::FIXED_HASH, group_bys, false}});
        sets.emplace_back(set);
    }
    return sets;
}

PropertySets DeterminerVisitor::visitApplyStep(const ApplyStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitEnforceSingleRowStep(const EnforceSingleRowStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitAssignUniqueIdStep(const AssignUniqueIdStep & node, DeterminerContext & context)
{
    return visitStep(node, context);
}

PropertySets DeterminerVisitor::visitCTERefStep(const CTERefStep &, DeterminerContext &)
{
    return {{}};
}

PropertySets DeterminerVisitor::visitExplainAnalyzeStep(const ExplainAnalyzeStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitTopNFilteringStep(const TopNFilteringStep &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

PropertySets DeterminerVisitor::visitFillingStep(const FillingStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitTableWriteStep(const TableWriteStep & step, DeterminerContext & context)
{
    auto node = Partitioning{Partitioning::Handle::FIXED_ARBITRARY};
    const auto * cnch_table = dynamic_cast<const StorageCnchMergeTree *>(step.getTarget()->getStorage().get());
    if (cnch_table && !cnch_table->supportsWriteInWorkers(context.getContext()))
    {
        // unique table can't support do TableWrite in many workers.
        node = Partitioning{Partitioning::Handle::SINGLE};
    }
    node.setComponent(Partitioning::Component::WORKER);
    return {{Property{node}}};
}

PropertySets DeterminerVisitor::visitTableFinishStep(const TableFinishStep &, DeterminerContext &)
{
    auto node = Partitioning{Partitioning::Handle::SINGLE};
    node.setComponent(Partitioning::Component::WORKER);
    return {{Property{node}}};
}

PropertySets DeterminerVisitor::visitOutfileWriteStep(const OutfileWriteStep &, DeterminerContext &)
{
    auto node = Partitioning{Partitioning::Handle::FIXED_ARBITRARY};
    node.setComponent(Partitioning::Component::WORKER);
    return {{Property{node}}};
}

PropertySets DeterminerVisitor::visitOutfileFinishStep(const OutfileFinishStep &, DeterminerContext &)
{
    auto node = Partitioning{Partitioning::Handle::SINGLE};
    node.setComponent(Partitioning::Component::WORKER);
    return {{Property{node}}};
}

PropertySets DeterminerVisitor::visitExpandStep(const ExpandStep &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

PropertySets DeterminerVisitor::visitIntermediateResultCacheStep(const IntermediateResultCacheStep & step, DeterminerContext & ctx)
{
    return visitStep(step, ctx);
}

}
