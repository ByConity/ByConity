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

#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/PlanVisitor.h>

#include <unordered_set>

namespace DB
{
using CTEId = UInt32;

class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;

/**
 * visit each cte def only once, when its cte-ref node has been first visited.
 */
template <typename R>
class SimpleCTEVisitHelper
{
public:
    explicit SimpleCTEVisitHelper(CTEInfo & cte_info_) : cte_info(cte_info_)
    {
    }

    template <typename C>
    R accept(CTEId id, PlanNodeVisitor<R, C> & visitor, C & context)
    {
        auto it = visit_results.find(id);
        if (it != visit_results.end())
            return it->second;
        auto res = VisitorUtil::accept(cte_info.getCTEDef(id), visitor, context);
        visit_results.emplace(id, res);
        return res;
    }

    template <typename C>
    R acceptAndUpdate(CTEId id, PlanNodeVisitor<R, C> & visitor, C & context)
    {
        auto it = visit_results.find(id);
        if (it != visit_results.end())
            return it->second;
        auto res = VisitorUtil::accept(cte_info.getCTEDef(id), visitor, context);
        cte_info.update(id, res);
        visit_results.emplace(id, res);
        return res;
    }

    template <typename C>
    R acceptAndUpdate(CTEId id, PlanNodeVisitor<R, C> & visitor, C & context, std::function<PlanNodePtr(R &)> && proj_func)
    {
        auto it = visit_results.find(id);
        if (it != visit_results.end())
            return it->second;
        auto res = VisitorUtil::accept(cte_info.getCTEDef(id), visitor, context);
        cte_info.update(id, proj_func(res));
        visit_results.emplace(id, res);
        return res;
    }

    CTEInfo & getCTEInfo() { return cte_info; }
    bool hasVisited(CTEId cte_id)
    {
        return visit_results.contains(cte_id);
    }

private:
    CTEInfo & cte_info;
    std::unordered_map<CTEId, R> visit_results;
};

/**
 * Ignore return value, just visit plan node
 */
template <>
class SimpleCTEVisitHelper<void>
{
public:
    explicit SimpleCTEVisitHelper(CTEInfo & cte_info_) : cte_info(cte_info_)
    {
    }

    template <typename R, typename C>
    void accept(CTEId id, PlanNodeVisitor<R, C> & visitor, C & context)
    {
        if (!visit_flags.emplace(id).second)
            return;
        VisitorUtil::accept(cte_info.getCTEDef(id), visitor, context);
    }

    CTEInfo & getCTEInfo()
    {
        return cte_info;
    }
    bool hasVisited(CTEId cte_id)
    {
        return visit_flags.contains(cte_id);
    }

private:
    CTEInfo & cte_info;
    std::unordered_set<CTEId> visit_flags;
};

/**
 * visit each cte def only once, when all its all cte-ref nodes have been visited.
 */
class PostorderCTEVisitHelper
{
public:
    explicit PostorderCTEVisitHelper(CTEInfo & cte_info_, PlanNodePtr & root)
        : cte_info(cte_info_), cte_reference_counts(cte_info.collectCTEReferenceCounts(root))
    {
    }

    template <typename R, typename C>
    void accept(CTEId id, PlanNodeBase & plan_node_id, PlanNodeVisitor<R, C> & visitor, C & context)
    {
        auto & plan_nodes = visit_flags[id];
        plan_nodes.emplace(plan_node_id);
        if (plan_nodes.size() == cte_reference_counts.at(id))
            VisitorUtil::accept(cte_info.getCTEDef(id), visitor, context);
    }

    template <typename C>
    PlanNodePtr acceptAndUpdate(CTEId id, PlanNodeId plan_node_id, PlanNodeVisitor<PlanNodePtr, C> & visitor, C & context)
    {
        auto & plan_nodes = visit_flags[id];
        plan_nodes.emplace(plan_node_id);

        auto & cte_def = cte_info.getCTEDef(id);
        if (plan_nodes.size() == cte_reference_counts.at(id))
            cte_def = VisitorUtil::accept(cte_def, visitor, context);
        return cte_def;
    }

    CTEInfo & getCTEInfo()
    {
        return cte_info;
    }

private:
    CTEInfo & cte_info;
    const std::unordered_map<CTEId, UInt64> cte_reference_counts;
    std::unordered_map<CTEId, std::unordered_set<PlanNodeId>> visit_flags;
};
}
