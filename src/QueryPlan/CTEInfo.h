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

#include <memory>
#include <set>
#include <unordered_map>

namespace DB
{
using CTEId = UInt32;

class PlanNodeBase;
using PlanNodePtr = std::shared_ptr<PlanNodeBase>;

class CTEInfo
{
public:
    PlanNodePtr & getCTEDef(CTEId cte_id) { return common_table_expressions.at(cte_id); }

    bool contains(CTEId cte_id) const { return common_table_expressions.contains(cte_id); }

    const std::unordered_map<CTEId, PlanNodePtr> & getCTEs() const { return common_table_expressions; }
    std::unordered_map<CTEId, PlanNodePtr> & getCTEs() { return common_table_expressions; }

    void add(CTEId id, PlanNodePtr plan);

    void update(CTEId id, PlanNodePtr plan);

    bool empty() const { return common_table_expressions.empty(); }

    size_t size() const { return common_table_expressions.size(); }

    void clear() { common_table_expressions.clear(); }

    std::unordered_map<CTEId, UInt64> collectCTEReferenceCounts(PlanNodePtr & root);

    std::set<CTEId> getCTEIds() const;

    CTEId nextCTEId() { return ++next_cte_id; }

private:
    std::unordered_map<CTEId, PlanNodePtr> common_table_expressions;

    CTEId next_cte_id = 0;

    class ReferenceCountsVisitor;
};
}
