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

#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>

namespace DB
{
class ColumnPruning : public Rewriter
{
public:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override { return "ColumnPruning"; }
    static void selectColumnWithMinSize(NamesAndTypesList source_columns, StoragePtr storage, NameSet & required);
};

class ColumnPruningVisitor : public PlanNodeVisitor<PlanNodePtr, NameSet>
{
public:
    explicit ColumnPruningVisitor(ContextMutablePtr context_, CTEInfo & cte_info_, PlanNodePtr & root)
        : context(std::move(context_)), post_order_cte_helper(cte_info_, root)
    {
    }

private:

    PlanNodePtr visitPlanNode(PlanNodeBase & node, NameSet & require) override;

#define VISITOR_DEF(TYPE) PlanNodePtr visit##TYPE##Node(TYPE##Node &, NameSet &) override;
    APPLY_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

    template <bool require_all>
    PlanNodePtr visitDefault(PlanNodeBase & node, NameSet & require);

    ContextMutablePtr context;
    CTEPostorderVisitHelper post_order_cte_helper;
    std::unordered_map<CTEId, NameSet> cte_require_columns{};
};

}
