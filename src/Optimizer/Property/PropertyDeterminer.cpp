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

#include <Optimizer/Property/PropertyDeterminer.h>

#include <Optimizer/Utils.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/UnionStep.h>
#include <QueryPlan/WindowStep.h>

namespace DB
{
PropertySets PropertyDeterminer::determineRequiredProperty(ConstQueryPlanStepPtr step, const Property & property)
{
    DeterminerContext context{property};
    static DeterminerVisitor visitor{};
    return VisitorUtil::accept(step, visitor, context);
}

PropertySets PropertyDeterminer::determineRequiredProperty(
    ConstQueryPlanStepPtr step, const Property & property, const std::vector<std::unordered_set<CTEId>> & child_with_clause)
{
    auto input_properties = determineRequiredProperty(step, property);
    for (auto & property_set : input_properties)
    {
        for (size_t i = 0; i < property_set.size(); ++i)
        {
            auto cte_descriptions = property.getCTEDescriptions().filter(child_with_clause[i]);
            property_set[i].setCTEDescriptions(std::move(cte_descriptions));
        }
    }
    return input_properties;
}

PropertySets DeterminerVisitor::visitStep(const IQueryPlanStep &, DeterminerContext & context)
{
    return {{context.getRequired()}};
}

PropertySets DeterminerVisitor::visitProjectionStep(const ProjectionStep & step, DeterminerContext & ctx)
{
    auto assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    auto translated = ctx.getRequired().translate(identities);
    if (!step.getInputStreams()[0].header)
        return {{Property{}}};
    translated.setPreferred(true);
    return {{translated}};
}

PropertySets DeterminerVisitor::visitFilterStep(const FilterStep &, DeterminerContext & context)
{
    auto require = context.getRequired();
    require.setPreferred(true);
    return {{require}};
}

// TODO property expand @jingpeng
PropertySets DeterminerVisitor::visitJoinStep(const JoinStep & step, DeterminerContext &)
{
    Names left_keys = step.getLeftKeys();
    Names right_keys = step.getRightKeys();

    // process ASOF join, it is different with normal join as the last keys is inequality
    if (step.getStrictness() == ASTTableJoin::Strictness::Asof)
    {
        if (!left_keys.empty()) left_keys.pop_back();
        if (!right_keys.empty()) right_keys.pop_back();
        Property left{Partitioning{Partitioning::Handle::FIXED_HASH, std::move(left_keys), false, 0, nullptr, true}};
        Property right{Partitioning{Partitioning::Handle::FIXED_HASH, std::move(right_keys), false, 0, nullptr, false}};
        PropertySets sets(1);
        sets[0].emplace_back(std::move(left));
        sets[0].emplace_back(std::move(right));
        return sets;
    }

    if (step.getDistributionType() == DistributionType::BROADCAST)
    {
        return {
            {Property{Partitioning{Partitioning::Handle::ARBITRARY}}, Property{Partitioning{Partitioning::Handle::FIXED_BROADCAST}}}};
    }

    if (left_keys.empty() && right_keys.empty())
    {
        Property left{Partitioning{Partitioning::Handle::SINGLE}};
        Property right{Partitioning{Partitioning::Handle::SINGLE}};
        PropertySets sets(1);
        sets[0].emplace_back(std::move(left));
        sets[0].emplace_back(std::move(right));
        return sets;
    }

    Property left{Partitioning{Partitioning::Handle::FIXED_HASH, std::move(left_keys), false, 0, nullptr, true}};
    Property right{Partitioning{Partitioning::Handle::FIXED_HASH, std::move(right_keys), false, 0, nullptr, false}};
    PropertySets sets(1);
    sets[0].emplace_back(std::move(left));
    sets[0].emplace_back(std::move(right));
    return sets;
}

PropertySets DeterminerVisitor::visitAggregatingStep(const AggregatingStep & step, DeterminerContext &)
{
//    if (/*step.isTotals() || */)
//    {
//        return {{Property{Partitioning{Partitioning::Handle::SINGLE}}}};
//    }

    auto keys = step.getKeys();
    PropertySets sets;
    if (keys.empty())
    {
        sets.resize(1);
        sets[0].emplace_back(Property{Partitioning{Partitioning::Handle::SINGLE}});
    }
    else
    {
        sets.emplace_back(PropertySet{Property{Partitioning{
            Partitioning::Handle::FIXED_HASH,
            std::move(keys),
        }}});

        if (step.isGroupingSet())
        {
            keys.emplace_back("__grouping_set");
            sets.emplace_back(PropertySet{Property{Partitioning{
                Partitioning::Handle::FIXED_HASH,
                std::move(keys),
            }}});
        }
    }

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
    PropertySets sets;
    sets[0].emplace_back(Property{Partitioning{
        Partitioning::Handle::FIXED_HASH,
        std::move(keys),
    }});
    return sets;
}

PropertySets DeterminerVisitor::visitUnionStep(const UnionStep & step, DeterminerContext & context)
{
    PropertySets sets(1);
    for (size_t i = 0; i < step.getInputStreams().size(); ++i)
    {
        std::unordered_map<String, String> mapping;
        for (const auto & output_to_input : step.getOutToInputs())
        {
            mapping[output_to_input.first] = output_to_input.second[i];
        }
        Property translated = context.getRequired().translate(mapping);
        translated.setPreferred(true);
        sets[0].emplace_back(std::move(translated));
    }
    return sets;
}

PropertySets DeterminerVisitor::visitIntersectStep(const IntersectStep & node, DeterminerContext & context)
{
    return visitStep(node, context);
}

PropertySets DeterminerVisitor::visitExceptStep(const ExceptStep & node, DeterminerContext & context)
{
    return visitStep(node, context);
}

PropertySets DeterminerVisitor::visitExchangeStep(const ExchangeStep & node, DeterminerContext & context)
{
    return visitStep(node, context);
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
PropertySets DeterminerVisitor::visitLimitByStep(const LimitByStep & node, DeterminerContext & context)
{
    return visitStep(node, context);
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
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitExtremesStep(const ExtremesStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}}};
}
//PropertySets DeterminerVisitor::visitFinalSamplingStep(const FinalSamplingStep &, DeterminerContext &)
//{
//    return {{Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}}};
//}

PropertySets DeterminerVisitor::visitWindowStep(const WindowStep & step, DeterminerContext &)
{
    const auto & keys = step.getWindow().partition_by;
    PropertySets sets(1);
    if (keys.empty())
    {
        sets[0].emplace_back(Property{Partitioning{Partitioning::Handle::SINGLE}});
    }
    else
    {
        Strings window_keys;
        window_keys.reserve(keys.size());
        for (const auto & key : keys)
        {
            window_keys.emplace_back(key.column_name);
        }
        sets[0].emplace_back(Property{Partitioning{Partitioning::Handle::FIXED_HASH, std::move(window_keys), false}});
    }
    return sets;
}

PropertySets DeterminerVisitor::visitApplyStep(const ApplyStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitEnforceSingleRowStep(const EnforceSingleRowStep &, DeterminerContext &)
{
    return {{Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}}};
}

PropertySets DeterminerVisitor::visitAssignUniqueIdStep(const AssignUniqueIdStep & node, DeterminerContext & context)
{
    return visitStep(node, context);
}

PropertySets DeterminerVisitor::visitCTERefStep(const CTERefStep &, DeterminerContext &)
{
    return {{}};
}

}
