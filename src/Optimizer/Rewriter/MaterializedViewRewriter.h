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

#include <Common/Logger.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <Optimizer/MaterializedView/PartitionConsistencyChecker.h>
#include <Optimizer/Property/Equivalences.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTTableColumnReference.h>
#include <QueryPlan/Assignment.h>

namespace DB
{
/**
  * MaterializedViewRewriter is based on "Optimizing Queries Using Materialized Views:
  * A Practical, Scalable Solution" by Goldstein and Larson.
  */
class MaterializedViewRewriter : public Rewriter
{
public:
    String name() const override { return "MaterializedViewRewriter"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;

    bool rewriteImpl(QueryPlan & plan, ContextMutablePtr context) const;

    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_materialized_view_rewrite || context->getSettingsRef().enable_view_based_query_rewrite;
    }

    LinkedHashMap<MaterializedViewStructurePtr, PartitionCheckResult>
    getRelatedMaterializedViews(QueryPlan & plan, ContextMutablePtr context) const;

    LoggerPtr log = getLogger("MaterializedViewRewriter");
};
}
