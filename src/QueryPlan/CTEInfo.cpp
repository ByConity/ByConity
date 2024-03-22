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

#include <QueryPlan/CTEInfo.h>

#include <QueryPlan/CTEVisitHelper.h>

#include <algorithm>

namespace DB
{
class CTEInfo::ReferenceCountsVisitor : public PlanNodeVisitor<Void, std::unordered_map<CTEId, UInt64>>
{
public:
    explicit ReferenceCountsVisitor(CTEInfo & cte_info_) : cte_helper(cte_info_) { }

    Void visitPlanNode(PlanNodeBase & node, std::unordered_map<CTEId, UInt64> & c) override
    {
        for (auto & child : node.getChildren())
            VisitorUtil::accept(*child, *this, c);
        return Void{};
    }

    Void visitCTERefNode(CTERefNode & node, std::unordered_map<CTEId, UInt64> & reference_counts) override {
        const auto * cte_step = dynamic_cast<const CTERefStep *>(node.getStep().get());
        auto cte_id = cte_step->getId();
        ++reference_counts[cte_id];
        cte_helper.accept(cte_id, *this, reference_counts);
        return Void{};
    }
private:
    SimpleCTEVisitHelper<void> cte_helper;
};

std::unordered_map<CTEId, UInt64> CTEInfo::collectCTEReferenceCounts(PlanNodePtr & root)
{
    ReferenceCountsVisitor visitor {*this};
    std::unordered_map<CTEId, UInt64> reference_counts;
    VisitorUtil::accept(*root, visitor, reference_counts);
    return reference_counts;
}

std::set<CTEId> CTEInfo::getCTEIds() const
{
    std::set<CTEId> cte_ids;
    for (const auto & item : common_table_expressions)
        cte_ids.emplace(item.first);
    return cte_ids;
}

void CTEInfo::update(CTEId id, PlanNodePtr plan)
{
    if (!contains(id))
        throw Exception("CTE " + std::to_string(id) + " don't exists", ErrorCodes::LOGICAL_ERROR);

    common_table_expressions[id] = std::move(plan);
}

void CTEInfo::add(CTEId id, PlanNodePtr plan)
{
    if (contains(id))
        throw Exception("CTE " + std::to_string(id) + " already exists", ErrorCodes::LOGICAL_ERROR);

    common_table_expressions[id] = std::move(plan);
    next_cte_id = std::max(id, next_cte_id);
}
}

