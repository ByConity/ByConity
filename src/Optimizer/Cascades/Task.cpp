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

#include <Optimizer/Cascades/Task.h>

#include <Interpreters/Context.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Cascades/Group.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <Optimizer/Property/Constants.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Property/PropertyDeriver.h>
#include <Optimizer/Property/PropertyDeterminer.h>
#include <Optimizer/Property/PropertyEnforcer.h>
#include <Optimizer/Property/PropertyMatcher.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>
#include <common/logger_useful.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/IQueryPlanStep.h>

#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int OPTIMIZER_TIMEOUT;
}

void OptimizeGroup::execute()
{
    // LOG_TRACE(log, "Optimize Group {}", group->getId());

    // Skip if
    // - This group has been optimized and has winner for given the context.
    // - This group has been optimized but doesn't have winner, and this task has more strict cost bound.
    //   eg, if group has been with CostUpperBound = 10 and no winner, neither has no winner for CostUpperBound = 5.
    double cost_lower_bound = group->getCostLowerBound(context->getRequiredProp());
    if (cost_lower_bound >= context->getCostUpperBound() || // Cost LB >= Cost UB
        group->hasWinner(context->getRequiredProp())) // Has optimized given the context
    {
        // LOG_TRACE(log, "Skip {}", group->getId());
        return;
    }

    // Update group cost LB
    if (cost_lower_bound < context->getCostUpperBound())
    {
        group->setCostLowerBound(context->getRequiredProp(), context->getCostUpperBound());
    }

    // Push explore task first for logical expressions if the group has not been optimized
    if (!group->hasExplored() || group->getPhysicalExpressions().empty())
    {
        for (const auto & logical_expr : group->getLogicalExpressions())
        {
            pushTask(std::make_shared<OptimizeExpression>(logical_expr, context));
        }
    }

    // Push implement tasks to ensure that they are run first (optional, for early pruning)
    for (const auto & physical_expr : group->getPhysicalExpressions())
    {
        pushTask(std::make_shared<OptimizeInput>(physical_expr, context));
    }

    // Since there is no cycle in the tree, it is safe to set the flag even before
    // all expressions are explored
    group->setExplorationFlag();
}

void OptimizeExpression::execute()
{
    // LOG_TRACE(log, "Optimize GroupExpr {}", group_expr->getGroupId());
    std::vector<RulePtr> valid_rules;

    // Construct valid transformation rules from rule set
    auto logical_rules = getTransformationRules();
    auto phys_rules = getImplementationRules();
    // If there are no stats, we won't enum plan
    if (group_expr->getChildrenGroups().empty())
    {
        constructValidRules(group_expr, logical_rules, valid_rules);
    }
    else if (context->getMemo().getGroupById(group_expr->getChildrenGroups()[0])->getStatistics())
    {
        auto stat_ptr = context->getMemo().getGroupById(group_expr->getChildrenGroups()[0])->getStatistics().value_or(nullptr);
        if (stat_ptr && !stat_ptr->getSymbolStatistics().empty())
            constructValidRules(group_expr, logical_rules, valid_rules);
    }
    constructValidRules(group_expr, phys_rules, valid_rules);

    // Apply rule
    for (auto & r : valid_rules)
    {
        pushTask(std::make_shared<ApplyRule>(group_expr, r, context));
        int child_group_idx = 0;
        const auto & pattern = r->getPattern();
        for (const auto * child_pattern : pattern->getChildrenPatterns())
        {
            // If child_pattern has any more children (i.e non-leaf), then we will explore the
            // child before applying the rule. (assumes task pool is effectively a stack)
            if (!child_pattern->getChildrenPatterns().empty() || child_pattern->getTargetType() == IQueryPlanStep::Type::Tree)
            {
                auto group = context->getMemo().getGroupById(group_expr->getChildrenGroups()[child_group_idx]);
                if (!group->hasExplored())
                {
                    pushTask(std::make_shared<ExploreGroup>(group, context));
                }
            }

            child_group_idx++;
        }
    }
}

void ExploreGroup::execute()
{
    // LOG_TRACE(log, "Explore Group {}", group->getId());

    if (group->hasExplored())
        return;

    for (const auto & logical_expr : group->getLogicalExpressions())
    {
        pushTask(std::make_shared<ExploreExpression>(logical_expr, context));
    }

    // Since there is no cycle in the tree, it is safe to set the flag even before
    // all expressions are explored
    group->setExplorationFlag();
}

void ExploreExpression::execute()
{
    // LOG_TRACE(log, "Explore GroupExpr {}", group_expr->getGroupId());
    std::vector<RulePtr> valid_rules;

    // Construct valid transformation rules from rule set
    auto logical_rules = getTransformationRules();
    // If there are no stats, we won't enum plan
    if (group_expr->getChildrenGroups().empty() || context->getMemo().getGroupById(group_expr->getChildrenGroups()[0])->getStatistics())
    {
        constructValidRules(group_expr, logical_rules, valid_rules);
    }

    // Apply rule
    for (auto & r : valid_rules)
    {
        pushTask(std::make_shared<ApplyRule>(group_expr, r, context, true));
        int child_group_idx = 0;
        const auto & pattern = r->getPattern();
        for (const auto * child_pattern : pattern->getChildrenPatterns())
        {
            // Only need to explore non-leaf children before applying rule to the
            // current group. this condition is important for early-pruning
            if (!child_pattern->getChildrenPatterns().empty() || child_pattern->getTargetType() == IQueryPlanStep::Type::Tree)
            {
                auto group = context->getMemo().getGroupById(group_expr->getChildrenGroups()[child_group_idx]);
                if (!group->hasExplored())
                {
                    pushTask(std::make_shared<ExploreGroup>(group, context));
                }
            }

            child_group_idx++;
        }
    }
}

void ApplyRule::execute()
{
    // LOG_TRACE(log, "Apply GroupExpr {}", group_expr->getGroupId());
    Stopwatch stop_watch{CLOCK_THREAD_CPUTIME_ID};
    
    if (context->getOptimizerContext().isEnableTrace())
        stop_watch.start();

    if (group_expr->hasRuleExplored(rule->getType()))
        return;

    const auto & pattern = rule->getPattern();
    GroupExprBindingIterator iterator(context->getMemo(), group_expr, pattern.get(), context);

    bool matched = false;

    RuleContext rule_context{
        context->getOptimizerContext().getContext(), context->getOptimizerContext().getCTEInfo(), context, group_expr->getGroupId()};
    
    while (iterator.hasNext())
    {
        auto before = iterator.next();
        assert(rule->getPattern()->matches(before));

        matched = true;
        // Caller frees after
        TransformResult result = rule->transform(before, rule_context);

        if (result.isEraseAll())
        {
            auto group = context->getOptimizerContext().getMemo().getGroupById(group_expr->getGroupId());
            group->deleteAllExpression();
        }
        else if (result.isEraseOld())
        {
            auto group = context->getOptimizerContext().getMemo().getGroupById(group_expr->getGroupId());
            group->deleteExpression(group_expr);
        }

        for (const auto & new_expr : result.getPlans())
        {
            GroupExprPtr new_group_expr = nullptr;
            auto g_id = group_expr->getGroupId();
            auto logical_rule = new_expr->getStep()->isLogical() ? rule->getType() : group_expr->getProduceRule();
            if (context->getOptimizerContext().recordPlanNodeIntoGroup(new_expr, new_group_expr, logical_rule, g_id))
            {
                // LOG_TRACE(log, "Success Apply Rule For Expression In Group {}; Rule Type: {}", group_expr->getGroupId(), rule->getType());

                for (auto type : rule->blockRules())
                {
                    new_group_expr->setRuleExplored(type);
                }
                // A new group expression is generated
                if (new_group_expr->isLogical())
                {
                    if (explore_only)
                    {
                        // Explore this logical expression
                        pushTask(std::make_shared<ExploreExpression>(new_group_expr, context));
                    }
                    else
                    {
                        // Optimize this logical expression
                        pushTask(std::make_shared<OptimizeExpression>(new_group_expr, context));
                    }
                }
                if (new_group_expr->isPhysical())
                {
                    // Cost this physical expression and optimize its inputs
                    pushTask(std::make_shared<OptimizeInput>(new_group_expr, context));
                }
            }
        }
    }

    group_expr->setRuleExplored(rule->getType());


    if (context->getOptimizerContext().isEnableTrace())
    {
        stop_watch.stop();
        if (matched)
            context->getOptimizerContext().trace("ApplyRule", group_expr->getGroupId(), rule->getType(), stop_watch.elapsedNanoseconds());
        else
            context->getOptimizerContext().trace("ApplyRule-Unmatched", group_expr->getGroupId(), rule->getType(), stop_watch.elapsedNanoseconds());
    }
}

void OptimizeInput::execute()
{
    // LOG_TRACE(log, "Optimize Input GroupExpr {} {}", group_expr->getGroupId(), context->getRequiredProp().toString());

    StopwatchGuard<Stopwatch> stop_watch_guard(elapsed_ns);
    stop_watch_guard.start();

    // Init logic: only run once per task
    if (cur_child_idx == -1)
    {
        // 1. We can init input cost using non-zero value for pruning
        // 2. We can calculate the current operator cost if we have maintain
        //    logical properties in group (e.g. stats, schema, cardinality)

        // Compute the cost of the root operator
        // 1. Collect stats needed and cache them in the group
        // 2. Calculate cost based on children's stats and cache it in the group expression
        if (!group_expr->isCostDerived())
        {
            auto cur_group = context->getMemo().getGroupById(group_expr->getGroupId());
            auto group_stats = cur_group->getStatistics().value_or(nullptr);

            std::vector<PlanNodeStatisticsPtr> children_stats;
            for (const auto & child : group_expr->getChildrenGroups())
                children_stats.emplace_back(context->getMemo().getGroupById(child)->getStatistics().value_or(nullptr));

            double cost = CostCalculator::calculate(
                              group_expr->getStep(),
                              group_stats,
                              children_stats,
                              *context->getOptimizerContext().getContext(),
                              context->getOptimizerContext().getWorkerSize())
                              .getCost(context->getOptimizerContext().getCostModel());

            group_expr->setCost(cost);
        }

        // Pruning
        if (group_expr->getCost() > context->getCostUpperBound())
        {
            // LOG_TRACE(log, "Pruned {}", group_expr->getGroupId());
            return;
        }

        // Forward to OptimizeCTE
        if (group_expr->getStep()->getType() == IQueryPlanStep::Type::CTERef)
        {
            pushTask(std::make_shared<OptimizeCTE>(group_expr, context));
            return;
        }

        // Collecte ctes children contains
        std::unordered_set<CTEId> visited_cte;
        for (auto child_group_id : group_expr->getChildrenGroups())
        {
            const auto & cte_set = context->getMemo().getGroupById(child_group_id)->getCTESet();
            input_cte_ids.emplace_back(cte_set);

            // Check Whether this group is common ancestor of cte.
            for (const auto & cte_id : cte_set)
                if (!visited_cte.emplace(cte_id).second && !context->getRequiredProp().getCTEDescriptions().contains(cte_id))
                    cte_common_ancestor.emplace(cte_id);
        }

        // Derive input properties
        initInputProperties();

        cur_child_idx = 0;
    }

    // Loop over (output prop, input props) pair for the GroupExpression being optimized
    // (1) Cost children (if needed); or pick the best child expression (in terms of cost)
    // (2) Enforce any missing properties as required
    // (3) Update Group/Context metadata of expression + cost
    for (; cur_prop_pair_idx < static_cast<int>(input_properties.size()); cur_prop_pair_idx++)
    {
        if (cur_prop_pair_idx > 100)
        {
            LOG_ERROR(context->getOptimizerContext().getLog(), "too many input property pair");
            break;
        }
        auto & input_props = input_properties[cur_prop_pair_idx];

        // initial total cost
        if (cur_child_idx == 0)
        {
            cur_total_cost = group_expr->getCost();
        }

        int children_size = group_expr->getChildrenGroups().size();
        for (; cur_child_idx < children_size; cur_child_idx++)
        {
            auto & i_prop = input_props[cur_child_idx];
            auto child_group = context->getOptimizerContext().getMemo().getGroupById(group_expr->getChildrenGroups()[cur_child_idx]);

            // Check whether the child group is already optimized for the prop
            if (child_group->hasWinner(i_prop))
            { // Directly get back the best expr if the child group is optimized
                auto child_best_expr = child_group->getBestExpression(i_prop);
                cur_total_cost += child_best_expr->getCost();
                // LOG_TRACE(
                //   log, "Group {} Lower Cost: {} Upper Bound: {}", group_expr->getGroupId(), cur_total_cost, context->getCostUpperBound());
                if (cur_total_cost > context->getCostUpperBound())
                {
                    // LOG_TRACE(log, "Pruned {}", group_expr->getGroupId());
                    break;
                }
            }
            else if (prev_child_idx != cur_child_idx)
            { // We haven't optimized child group
                prev_child_idx = cur_child_idx;
                pushTask(this->shared_from_this());

                auto cost_high = context->getCostUpperBound() - cur_total_cost;
                // LOG_TRACE(
                //     log,
                //     "Create Group {} Upper Bound: {} from up {} cur {}",
                //     child_group->getId(),
                //     cost_high,
                //     context->getCostUpperBound(),
                //     cur_total_cost);
                auto ctx = std::make_shared<OptimizationContext>(context->getOptimizerContext(), i_prop, cost_high);
                pushTask(std::make_shared<OptimizeGroup>(child_group, ctx));
                return;
            }
            else
            { // If we return from OptimizeGroup, then there is no expr for the context
                break;
            }
        }

        // Check whether we successfully optimize all child group
        if (cur_child_idx == children_size)
        {
            PropertySet actual_input_props;
            std::vector<std::pair<CTEId, std::pair<Property, double>>> cte_actual_props;
            size_t single_count = 0;
            for (int index = 0; index < children_size; index++)
            {
                auto & i_prop = input_props[index];
                auto child_group = context->getOptimizerContext().getMemo().getGroupById(group_expr->getChildrenGroups()[index]);
                auto child_best_expr = child_group->getBestExpression(i_prop);

                actual_input_props.emplace_back(child_best_expr->getActualProperty());
                for (const auto & item : child_best_expr->getCTEActualProperties())
                    cte_actual_props.emplace_back(item);

                if (child_best_expr->getActualProperty().getNodePartitioning().getHandle() == Partitioning::Handle::SINGLE)
                    single_count++;
            }

            if (group_expr->getStep()->getType() == IQueryPlanStep::Type::Union && single_count > 0
                && single_count < group_expr->getChildrenGroups().size())
            {
                auto new_child_requires = input_props;
                for (auto & new_child : new_child_requires)
                {
                    new_child.setNodePartitioning(Partitioning{Partitioning::Handle::SINGLE});
                    new_child.setPreferred(false);
                }
                input_properties.emplace_back(new_child_requires);
                // Reset child idx and total cost
                prev_child_idx = -1;
                cur_child_idx = 0;
                cur_total_cost = 0;
                continue;
            }

            bool vaild = group_expr->getStep()->getType() == IQueryPlanStep::Type::Join
                ? checkJoinInputProperties(input_props, actual_input_props)
                : true;
            if (vaild)
            {
                Property output_prop = PropertyDeriver::deriveProperty(
                    group_expr->getStep(), actual_input_props, context->getRequiredProp(), context->getOptimizerContext().getContext());
                enforcePropertyAndUpdateWinner(
                    context, group_expr, std::move(output_prop), cur_total_cost, input_props, cte_common_ancestor, cte_actual_props);
            }
        }

        // Reset child idx and total cost
        prev_child_idx = -1;
        cur_child_idx = 0;
        cur_total_cost = 0;

        // Explore and derive all possible input properties
        if (cur_prop_pair_idx + 1 == static_cast<int>(input_properties.size()))
            exploreInputProperties();
    }

    stop_watch_guard.stop();
    if (context->getOptimizerContext().isEnableTrace())
        context->getOptimizerContext().trace("OptimizeInput", group_expr->getGroupId(), group_expr->getProduceRule(), elapsed_ns);
}

void OptimizeInput::initInputProperties()
{
    // initialize input properties with default required property.
    auto required_properties = PropertyDeterminer::determineRequiredProperty(
        group_expr->getStep(), context->getRequiredProp(), *context->getOptimizerContext().getContext());
    initPropertiesForCTE(required_properties);
    input_properties = std::move(required_properties);
}

void OptimizeInput::initPropertiesForCTE(PropertySets & required_properties)
{
    // init cte property
    for (const auto & cte_id : cte_common_ancestor)
    {
        auto & properties = explored_cte_properties[cte_id];
        auto cte_description = CTEDescription::from(
            PropertyMatcher::compatibleCommonRequiredProperty(context->getOptimizerContext().getCTEDefPropertyRequirements()[cte_id]));
        if (!cte_description.isArbitrary())
            cte_property_enumerated.emplace(cte_id);
        properties.emplace(cte_description);
    }

    // fill and filter cte property
    for (auto & properties : required_properties)
    {
        for (size_t i = 0; i < properties.size(); ++i)
        {
            auto & cte_descriptions = properties[i].getCTEDescriptions();
            for (const auto & cte_id : cte_common_ancestor)
                cte_descriptions[cte_id] = *explored_cte_properties[cte_id].begin();

            // even if this groupexpr is not cte common ancstor, we need filter cte descriptions using input_cte_ids
            cte_descriptions.filter(input_cte_ids[i]);
        }
    }
}

void OptimizeInput::exploreInputProperties()
{
    // explore shared cte as input properties requirements.
    for (const auto & cte_id : cte_common_ancestor)
    {
        if (!cte_property_enumerated.emplace(cte_id).second)
            continue;

        // explore cte common property
        auto & cte_def_required_properties = context->getOptimizerContext().getCTEDefPropertyRequirements()[cte_id];
        if (!cte_def_required_properties.empty())
        {
            if (context->getOptimizerContext().getContext()->getSettingsRef().enable_cte_property_enum)
            {
                // CTERef may require identical properties. These properties can be enforced in the CTEDef to
                // avoid repeated work.
                for (const auto & winner : cte_def_required_properties)
                    addInputPropertiesForCTE(cte_id, CTEDescription::from(winner));
            }

            if (context->getOptimizerContext().getContext()->getSettingsRef().enable_cte_common_property)
            {
                // It is too expensive to enumerate all possible properties, especially if there are lots CTERef.
                // We can only optimize for common property instead.
                auto common_property = PropertyMatcher::compatibleCommonRequiredProperty(cte_def_required_properties);
                addInputPropertiesForCTE(cte_id, CTEDescription::from(common_property));
            }
        }

        // explore cte inline
        if (!cte_inlined_enumerated && context->getOptimizerContext().isEnableAutoCTE())
        {
            cte_inlined_enumerated = true;
            addInputPropertiesForCTE(cte_id, CTEDescription::inlined());
        }
    }

    if (!cte_inlined_enumerated)
    {
        cte_inlined_enumerated = true;
        for (const auto & cte_id : cte_common_ancestor)
            addInputPropertiesForCTE(cte_id, CTEDescription::inlined());
    }
}

void OptimizeInput::addInputPropertiesForCTE(CTEId cte_id, CTEDescription cte_description)
{
    if (!explored_cte_properties[cte_id].emplace(cte_description).second)
        return;

    PropertySets new_properties;
    for (auto & children_properties : input_properties)
    {
        bool is_initial_properties = true;
        for (size_t i = 0; i < children_properties.size(); i++)
        {
            if (!input_cte_ids[i].contains(cte_id))
                continue;
            if (children_properties[i].getCTEDescriptions().at(cte_id) != input_properties[0][i].getCTEDescriptions().at(cte_id))
            {
                is_initial_properties = false;
                break;
            }
        }
        if (!is_initial_properties)
            continue;

        auto copy_children_properties = children_properties;
        for (size_t i = 0; i < copy_children_properties.size(); i++)
            if (input_cte_ids[i].contains(cte_id))
                copy_children_properties[i].getCTEDescriptions()[cte_id] = cte_description;
        new_properties.emplace_back(std::move(copy_children_properties));
    }
    input_properties.insert(input_properties.end(), new_properties.begin(), new_properties.end());
}

static PropertySets makeHandleSame(const PropertySet & input_props, const PropertySet & actual_props, const ContextPtr & context)
{
    PropertySets result;
    auto new_child_requires = input_props;
    for (auto & new_child : new_child_requires)
    {
        new_child.getNodePartitioningRef().setRequireHandle(true);
    }
    result.emplace_back(new_child_requires);

    if (actual_props[0].getNodePartitioning().isExchangeSchema(context->getSettingsRef().enable_bucket_shuffle)
        && actual_props[0].getNodePartitioning().getHandle() == Partitioning::Handle::BUCKET_TABLE)
    {
        auto other_new_child_requires = new_child_requires;
        for (auto & new_child : other_new_child_requires)
        {
            new_child.getNodePartitioningRef().setHandle(Partitioning::Handle::BUCKET_TABLE);
            new_child.getNodePartitioningRef().setBuckets(actual_props[0].getNodePartitioning().getBuckets());
            new_child.getNodePartitioningRef().setBucketExpr(actual_props[0].getNodePartitioning().getBucketExpr());
        }
        result.emplace_back(other_new_child_requires);
    }

    if (actual_props[1].getNodePartitioning().isExchangeSchema(context->getSettingsRef().enable_bucket_shuffle)
        && actual_props[1].getNodePartitioning().getHandle() == Partitioning::Handle::BUCKET_TABLE)
    {
        auto other_new_child_requires = new_child_requires;
        for (auto & new_child : other_new_child_requires)
        {
            new_child.getNodePartitioningRef().setHandle(Partitioning::Handle::BUCKET_TABLE);
            new_child.getNodePartitioningRef().setBuckets(actual_props[1].getNodePartitioning().getBuckets());
            new_child.getNodePartitioningRef().setBucketExpr(actual_props[1].getNodePartitioning().getBucketExpr());
        }
        result.emplace_back(other_new_child_requires);
    }
    return result;
}

bool OptimizeInput::checkJoinInputProperties(const PropertySet & requried_input_props, const PropertySet & actual_input_props)
{
    bool all_fix_hash = std::all_of(requried_input_props.begin(), requried_input_props.end(), [](const auto & i_prop) {
        return i_prop.getNodePartitioning().getHandle() == Partitioning::Handle::FIXED_HASH;
    });
    if (!all_fix_hash)
        return true;

    bool match = false;
    const auto & first_props = actual_input_props[0];
    if (first_props.getNodePartitioning().getHandle() == Partitioning::Handle::FIXED_HASH
        || first_props.getNodePartitioning().getHandle() == Partitioning::Handle::BUCKET_TABLE)
    {
        match = true;
        auto left_equivalences = context->getMemo().getGroupById(group_expr->getChildrenGroups()[0])->getEquivalences();
        auto right_equivalences = context->getMemo().getGroupById(group_expr->getChildrenGroups()[1])->getEquivalences();

        auto left_output_symbols
            = context->getMemo().getGroupById(group_expr->getChildrenGroups()[0])->getStep()->getOutputStream().header.getNameSet();
        auto right_output_symbols
            = context->getMemo().getGroupById(group_expr->getChildrenGroups()[1])->getStep()->getOutputStream().header.getNameSet();

        NameToNameSetMap right_join_key_to_left;
        DefaultTMap<String> before_left_rep_map, before_right_rep_map;
        if (const auto * join_step = dynamic_cast<const JoinStep *>(group_expr->getStep().get()))
        {
            auto left_rep_map = left_equivalences->representMap();
            auto right_rep_map = right_equivalences->representMap();
            before_left_rep_map = left_rep_map;
            before_right_rep_map = right_rep_map;
            for (size_t join_key_index = 0; join_key_index < join_step->getLeftKeys().size(); ++join_key_index)
            {
                auto left_key = join_step->getLeftKeys()[join_key_index];
                auto right_key = join_step->getRightKeys()[join_key_index];
                left_key = left_rep_map.count(left_key) ? left_rep_map.at(left_key) : left_key;
                right_key = right_rep_map.count(right_key) ? right_rep_map.at(right_key) : right_key;
                right_join_key_to_left[right_key].insert(left_key);
            }
        }

        auto first_handle = first_props.getNodePartitioning().getHandle();
        auto first_bucket_count = first_props.getNodePartitioning().getBuckets();
        auto first_sharding_expr = first_props.getNodePartitioning().getBucketExpr();
        auto first_partition_column = first_props.getNodePartitioning().normalize(*left_equivalences).getColumns();

        for (size_t actual_prop_index = 1; actual_prop_index < actual_input_props.size(); ++actual_prop_index)
        {
            auto before_transformed_partition_cols = actual_input_props[actual_prop_index].getNodePartitioning().getColumns();
            auto translated_prop = actual_input_props[actual_prop_index].normalize(*right_equivalences);
            if (translated_prop.getNodePartitioning().getHandle() != first_handle
                || (translated_prop.getNodePartitioning().getBuckets() != first_bucket_count && !(translated_prop.getNodePartitioning().isSatisfyWorker()
                            && first_props.getNodePartitioning().isSatisfyWorker()))
                || !ASTEquality::compareTree(translated_prop.getNodePartitioning().getBucketExpr(), first_sharding_expr))
            {
                match = false;
                break;
            }
            const auto & transformed_partition_cols = translated_prop.getNodePartitioning().getColumns();
            if (transformed_partition_cols.size() != first_partition_column.size())
            {
                match = false;
                break;
            }
            for (size_t col_index = 0; col_index < transformed_partition_cols.size(); col_index++)
            {
                if (right_join_key_to_left[transformed_partition_cols[col_index]].count(first_partition_column[col_index]) == 0)
                {
                    match = false;
                    break;
                }
            }
        }
    }

    if (!match)
    {
        for (auto & new_child_requires : makeHandleSame(requried_input_props, actual_input_props, context->getOptimizerContext().getContext()))
        {
            input_properties.emplace_back(new_child_requires);
        }
    }


    return match;
}


void OptimizeInput::enforcePropertyAndUpdateWinner(
    OptContextPtr & opt_context,
    GroupExprPtr group_expr,
    Property output_prop,
    double total_cost,
    const PropertySet & input_props,
    const std::set<CTEId> & cte_common_ancestor,
    const std::vector<std::pair<CTEId, std::pair<Property, double>>> & actual_intput_cte_props)
{
    // Not need to do pruning here because it has been done when we get the
    // best expr from the child group
    auto cur_group = opt_context->getMemo().getGroupById(group_expr->getGroupId());
    auto group_stats = cur_group->getStatistics().value_or(nullptr);
    auto equivalences = cur_group->getEquivalences();
    Constants constants = cur_group->getConstants().value_or(Constants{});

    // Enforce property if the requirement does not meet
    auto require = opt_context->getRequiredProp();

    GroupExprPtr remote_exchange;
    GroupExprPtr local_exchange;
    if (!require.getNodePartitioning().isPreferred()
        && !PropertyMatcher::matchNodePartitioning(
            *opt_context->getOptimizerContext().getContext(),
            require.getNodePartitioningRef(),
            output_prop.getNodePartitioning(),
            *equivalences,
            constants))
    {
        // add remote exchange
        remote_exchange
            = PropertyEnforcer::enforceNodePartitioning(group_expr, require, output_prop, *opt_context->getOptimizerContext().getContext());
        output_prop = PropertyDeriver::deriveProperty(
            remote_exchange->getStep(), output_prop, opt_context->getRequiredProp(), opt_context->getOptimizerContext().getContext());
        // add cost
        total_cost += CostCalculator::calculate(
                          remote_exchange->getStep(),
                          group_stats,
                          {group_stats},
                          *opt_context->getOptimizerContext().getContext(),
                          opt_context->getOptimizerContext().getWorkerSize())
                          .getCost(opt_context->getOptimizerContext().getCostModel());
    }

    if (!require.getStreamPartitioning().isPreferred()
        && !PropertyMatcher::matchStreamPartitioning(
            *opt_context->getOptimizerContext().getContext(),
            require.getStreamPartitioning(),
            output_prop.getStreamPartitioning(),
            *equivalences,
            constants,
            opt_context->getOptimizerContext().getContext()->getSettingsRef().enable_add_local_exchange))
    {
        // add local exchange
        local_exchange = PropertyEnforcer::enforceStreamPartitioning(
            remote_exchange ? remote_exchange : group_expr, require, output_prop, *opt_context->getOptimizerContext().getContext());
        output_prop = PropertyDeriver::deriveProperty(
            local_exchange->getStep(), output_prop, opt_context->getRequiredProp(), opt_context->getOptimizerContext().getContext());
        // add cost
        total_cost += CostCalculator::calculate(
                          local_exchange->getStep(),
                          group_stats,
                          {group_stats},
                          *opt_context->getOptimizerContext().getContext(),
                          opt_context->getOptimizerContext().getWorkerSize())
                          .getCost(opt_context->getOptimizerContext().getCostModel());
    }

    // Merge cte actual props
    std::map<CTEId, std::pair<Property, double>> cte_actual_props;
    for (const auto & cte_prop : actual_intput_cte_props)
    {
        auto it = cte_actual_props.emplace(cte_prop);

        // increase cost if the cte exists both join side. disable q11 & q74 cte for tpcds.
        if (!it.second && group_expr->getStep()->getType() == IQueryPlanStep::Type::Join)
        {
            auto coefficient
                = opt_context->getOptimizerContext().getContext()->getSettingsRef().cost_calculator_cte_weight_for_join_build_side;
            it.first->second.second = std::max(it.first->second.second, cte_prop.second.second) * coefficient;
        }
    }

    // Add cost for cte
    std::vector<CTEId> cte_ancestor;
    for (const auto & cte_prop : cte_actual_props)
    {
        if (cte_common_ancestor.contains(cte_prop.first))
        {
            total_cost += cte_prop.second.second;
            cte_ancestor.emplace_back(cte_prop.first);
        }
    }

    // Update winner
    if (total_cost <= opt_context->getCostUpperBound())
    {
        // If the cost is smaller than the winner, update the context upper bound
        if (total_cost < opt_context->getCostUpperBound())
        {
            // LOG_TRACE(opt_context->getOptimizerContext().getLog(), "Update Group {} Upper Bound to : {}", group_expr->getGroupId(), total_cost);
            opt_context->setCostUpperBound(total_cost);
        }

        if (!opt_context->getOptimizerContext().isEnableCbo())
            total_cost = 0;

        output_prop = output_prop.normalize(*equivalences);
        cur_group->setExpressionCost(
            std::make_shared<Winner>(
                group_expr, remote_exchange, local_exchange, input_props, output_prop, total_cost, cte_actual_props, cte_ancestor),
            opt_context->getRequiredProp());
    }
}


void OptimizeCTE::execute()
{
    StopwatchGuard<Stopwatch> stop_watch_guard(elapsed_ns);

    const auto * const cte_step = dynamic_cast<const CTERefStep *>(group_expr->getStep().get());
    CTEId cte_id = cte_step->getId();
    auto cte_def_group = context->getMemo().getCTEDefGroupByCTEId(cte_id);

    // 1. Check whether request property for this group_expr is invalid.
    if (context->getRequiredProp().getCTEDescriptions().contains(cte_id))
    {
        // It is invalid if cte require inline
        auto cte_description = context->getRequiredProp().getCTEDescriptions().at(cte_id);
        if (!cte_description.isShared())
        {
            LOG_TRACE(log, "Invalid {}", group_expr->getGroupId());
            return;
        }

        // It is invalid if cte ref don't require broadcast but cte def output property require broadcast
        if (cte_description.getNodePartitioning().getHandle() == Partitioning::Handle::FIXED_BROADCAST
            && context->getRequiredProp().getNodePartitioning().getHandle() != Partitioning::Handle::FIXED_BROADCAST)
        {
            LOG_TRACE(log, "Invalid {}: BROADCAST", group_expr->getGroupId());
            return;
        }
    }

    // 2. CTERefStep output property can not be determined locally, it has been determined globally,
    //  described in CTEDescription of property. If They don't match, we just ignore required property.
    // eg, input required property: <Repartition[B], CTE(0)=Repartition[A]> don't match,
    //    we ignore local required property Repartition[B] and prefer global property Repartition[A]
    auto cte_global_property = CTEDescription::createCTEDefGlobalProperty(context->getRequiredProp(), cte_id);

    // 3. if CTEDef group hasn't been optimized for global determined property, we submit it here.
    if (!cte_def_group->hasWinner(cte_global_property))
    {
        if (cte_def_group->hasOptimized(cte_global_property))
            return; // We have optimized cte group but there is still no valid plan.
        pushTask(this->shared_from_this());
        auto ctx = std::make_shared<OptimizationContext>(
            context->getOptimizerContext(), cte_global_property, std::numeric_limits<double>::max());
        pushTask(std::make_shared<OptimizeGroup>(cte_def_group, ctx));
        return;
    }

    // 4. We save local required property, as we could re-optimize cte common property later.
    auto local_required_property
        = CTEDescription::createCTEDefLocalProperty(context->getRequiredProp(), cte_id, cte_step->getOutputColumns());
    context->getOptimizerContext().getCTEDefPropertyRequirements()[cte_id].emplace(local_required_property);

    // 5. derive property and cost
    auto cte_def_best_expr = cte_def_group->getBestExpression(cte_global_property);
    Property output_prop = cte_def_best_expr->getActualProperty().translate(cte_step->getReverseOutputColumns());

    std::vector<std::pair<CTEId, std::pair<Property, double>>> cte_actual_props;
    cte_actual_props.emplace_back(std::make_pair(cte_id, std::make_pair(cte_global_property, cte_def_best_expr->getCost())));
    for (const auto & item : cte_def_best_expr->getCTEActualProperties())
        cte_actual_props.emplace_back(item);

    std::set<CTEId> cte_common_ancestor;
    // CTE may reference only once, so we have not explored in common ancestor.
    if (!context->getRequiredProp().getCTEDescriptions().contains(cte_id))
        cte_common_ancestor.emplace(cte_id);

    OptimizeInput::enforcePropertyAndUpdateWinner(
        context, group_expr, std::move(output_prop), group_expr->getCost(), {}, cte_common_ancestor, cte_actual_props);

    stop_watch_guard.stop();
    if (context->getOptimizerContext().isEnableTrace())
        context->getOptimizerContext().trace("OptimizeCTE", group_expr->getGroupId(), group_expr->getProduceRule(), elapsed_ns);
}

OptimizerTask::OptimizerTask(OptContextPtr context_) : context(std::move(context_)), log(context->getOptimizerContext().getLog())
{
}

void OptimizerTask::pushTask(const OptimizerTaskPtr & task)
{
    context->pushTask(task);
}

const std::vector<RulePtr> & OptimizerTask::getTransformationRules() const
{
    return context->getTransformationRules();
}

const std::vector<RulePtr> & OptimizerTask::getImplementationRules() const
{
    return context->getImplementationRules();
}

void OptimizerTask::constructValidRules(
    const GroupExprPtr & group_expr, const std::vector<RulePtr> & rules, std::vector<RulePtr> & valid_rules)
{
    for (const auto & rule : rules)
    {
        if (!rule->isEnabled(context->getOptimizerContext().getContext()))
        {
            continue;
        }
        // Check if we can apply the rule
        if (group_expr->hasRuleExplored(rule->getType()))
            continue;
        if (!rule->getTargetTypes().count(group_expr->getStep()->getType()))
        {
            // match head pattern
            continue;
        }

        // This check exists only as an "early" reject. As is evident, we do not check
        // the full pattern here. Checking the full pattern happens when actually trying to
        // apply the rule (via a GroupExprBindingIterator).
        auto child_pattern_size = rule->getPattern()->getChildrenPatterns().size();
        if (child_pattern_size > 0 && group_expr->getChildrenGroups().size() != child_pattern_size)
            continue;

        valid_rules.emplace_back(rule);
    }
}

}
