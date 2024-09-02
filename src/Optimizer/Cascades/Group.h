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

#pragma once

#include <Core/Types.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Optimizer/Property/Constants.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Property/SymbolEquivalencesDeriver.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/DataDependency/DataDependency.h>
#include <memory>
#include <string>

namespace DB
{
namespace ErrorCodes
{
    extern const int PLAN_BUILD_ERROR;
}

using GroupId = UInt32;

class Group;
using GroupPtr = std::shared_ptr<Group>;

class CascadesContext;
class Winner;

class Group
{
public:
    explicit Group(GroupId id_) : id(id_) { }

    GroupId getId() const { return id; }
    bool isSimpleChildren() const { return simple_children; }
    bool isTableScan() const { return is_table_scan; }
    bool isJoinRoot() const { return is_join_root; }
    UInt64 getMaxTableScans() const { return max_table_scans; }
    UInt64 getMaxTableScanRows() const { return max_table_scan_rows; }

    void addExpression(const GroupExprPtr & expression, CascadesContext & context);
    void makeRootJoinInfo(CascadesContext & context);
    void makeRootJoinInfo(GroupExpression & expression, CascadesContext & context);

    double getCostLowerBound(const Property & property) const;
    void setCostLowerBound(const Property & property, double lower_bound);
    bool hasOptimized(const Property & property) const;

    void deleteExpression(const GroupExprPtr & expression);
    void deleteAllExpression();

    /**
     * Gets the best expression existing for a group satisfying
     * a certain PropertySet. HasExpressions() should return TRUE.
     *
     * @param property_set PropertySet to use for search
     * @returns GroupExpression satisfing the PropertySet
     */
    WinnerPtr getBestExpression(const Property & property_set) const
    {
        if (auto it = lowest_cost_expressions.find(property_set); it != lowest_cost_expressions.end())
        {
            return it->second;
        }
        throw Exception(
            "Cascades can not build plan, Group " + std::to_string(id) + " " + property_set.toString(), ErrorCodes::PLAN_BUILD_ERROR);
    }

    bool hasWinner(const Property & property_set) const { return lowest_cost_expressions.contains(property_set); }

    /**
     * Sets metadata for the cost of an expression w.r.t PropertySet
     * @param expr GroupExpression whose metadata is to be updated
     * @param property PropertySet satisfied by GroupExpression
     * @returns TRUE if expr recorded
     *
     * @note properties becomes owned by Group!
     * @note properties lifetime after not guaranteed
     */
    bool setExpressionCost(const WinnerPtr & expr, const Property & property);

    const std::vector<GroupExprPtr> & getLogicalExpressions() const { return logical_expressions; }
    const std::vector<GroupExprPtr> & getPhysicalExpressions() const { return physical_expressions; }
    const std::vector<GroupExprPtr> & getLogicalOtherwisePhysicalExpressions() const
    {
        if (!logical_expressions.empty())
            return logical_expressions;
        if (!physical_expressions.empty())
            return physical_expressions;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cascades group must has at least one logical/physical expression!");
    }

    QueryPlanStepPtr & getStep() const
    {
        if (logical_expressions.empty())
            return physical_expressions[0]->getStep();
        return logical_expressions[0]->getStep();
    }
    PlanNodePtr createLeafNode(ContextMutablePtr context) const;

    /**
     * Sets a flag indicating the group has been explored
     */
    void setExplorationFlag() { has_explored = true; }

    /**
     * Checks whether this group has been explored yet.
     * @returns TRUE if explored
     */
    bool hasExplored() const { return has_explored; }

    bool isStatsDerived() const { return stats_derived; }

    const std::optional<PlanNodeStatisticsPtr> & getStatistics() const { return statistics; }

    const JoinSets & getJoinSets() const { return join_sets; }

    bool containsJoinSet(const JoinSet & join_set) const { return join_sets.contains(join_set); }
    void addJoinSet(const JoinSet & join_set) { join_sets.insert(join_set); }

    const std::unordered_map<Property, WinnerPtr, PropertyHash> & getLowestCostExpressions() const { return lowest_cost_expressions; }

    const std::optional<Constants> & getConstants() const
    {
        return constants;
    }

    const std::optional<DataDependency> & getDataDependency() const
    {
        return data_dependency;
    }

    const SymbolEquivalencesPtr & getEquivalences() const { return equivalences; }

    const std::unordered_set<CTEId> & getCTESet() const { return cte_set; }
    UInt32 getJoinRootId() const { return join_root_id; }
private:
    GroupId id = UNDEFINED_GROUP;

    /**
     * Mapping from property requirements to winner
     */
    std::unordered_map<Property, WinnerPtr, PropertyHash> lowest_cost_expressions;
    
    /**
     * Vector of equivalent logical expressions
     */
    std::vector<GroupExprPtr> logical_expressions;

    /**
     * Vector of equivalent physical expressions
     */
    std::vector<GroupExprPtr> physical_expressions;

    /**
     * Cost Lower Bound
     */
    std::unordered_map<Property, double, PropertyHash> cost_lower_bounds;

    /**
     * Whether equivalent logical expressions have been explored for this group
     */
    bool has_explored = false;

    bool stats_derived = false;

    std::optional<PlanNodeStatisticsPtr> statistics;

    JoinSets join_sets;

    std::unordered_set<CTEId> cte_set;

    UInt64 max_table_scans = 0;
    UInt64 max_table_scan_rows = 0;

    SymbolEquivalencesPtr equivalences;
    std::optional<Constants> constants;
    std::optional<DataDependency> data_dependency;

    bool simple_children = true;
    bool is_table_scan = false;
    bool is_join_root = false;
    UInt32 join_root_id = 0;
};

}
