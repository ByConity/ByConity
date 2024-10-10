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
#include <Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/prepared_statement.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/PlanVisitor.h>
#include <Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Interpreters/QueryLog.h>
#include <QueryPlan/QueryPlan.h>
#include <Poco/Logger.h>
#include <Common/Stopwatch.h>
#include "Parsers/IAST_fwd.h"

namespace Poco
{
class Logger;
}

namespace DB
{
struct Analysis;
using AnalysisPtr = std::shared_ptr<Analysis>;
struct QueryCacheContext;

class InterpreterSelectQueryUseOptimizer : public IInterpreter
{
public:
    InterpreterSelectQueryUseOptimizer(const ASTPtr & query_ptr_, ContextMutablePtr context_, const SelectQueryOptions & options_)
        : InterpreterSelectQueryUseOptimizer(query_ptr_, nullptr, {}, context_, options_)
    {
    }

    InterpreterSelectQueryUseOptimizer(
        PlanNodePtr sub_plan_ptr_, CTEInfo cte_info_, ContextMutablePtr context_, const SelectQueryOptions & options_)
        : InterpreterSelectQueryUseOptimizer(nullptr, std::move(sub_plan_ptr_), std::move(cte_info_), context_, options_)
    {
    }

    InterpreterSelectQueryUseOptimizer(
        const ASTPtr & query_ptr_,
        PlanNodePtr sub_plan_ptr_,
        CTEInfo cte_info_,
        ContextMutablePtr & context_,
        const SelectQueryOptions & options_);

    QueryPlanPtr getQueryPlan(bool skip_optimize = false);
    void buildQueryPlan(QueryPlanPtr & query_plan, AnalysisPtr & analysis, bool skip_optimize = false);
    std::pair<PlanSegmentTreePtr, std::set<StorageID>> getPlanSegment();
    QueryPlanPtr getPlanFromCache(UInt128 query_hash);
    bool addPlanToCache(UInt128 query_hash, QueryPlanPtr & plan, AnalysisPtr analysis);
    static void setPlanSegmentInfoForExplainAnalyze(PlanSegmentTreePtr & plan_segment_tree, ContextMutablePtr context);
    BlockIO readFromQueryCache(ContextPtr local_context, QueryCacheContext & can_use_query_cache);

    BlockIO execute() override;

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const override
    {
        elem.query_kind = "Select";
        elem.segment_profiles = segment_profiles;
    }

    static void resetFinalSampleSize(PlanSegmentTreePtr & plan_segment_tree);

    static void fillContextQueryAccessInfo(ContextPtr context, AnalysisPtr & analysis);

    static void fillQueryPlan(ContextPtr context, QueryPlan & query_plan);

    Block getSampleBlock();

    static void setUnsupportedSettings(ContextMutablePtr & context);

    std::optional<std::set<StorageID>> getUsedStorageIds();
    BlockIO executeCreatePreparedStatementQuery();
    bool isCreatePreparedStatement();

    ASTPtr & getQuery() { return query_ptr; }

private:
    ASTPtr query_ptr;
    PlanNodePtr sub_plan_ptr;
    CTEInfo cte_info;
    ContextMutablePtr context;
    SelectQueryOptions options;
    LoggerPtr log;
    bool interpret_sub_query;
    PlanSegmentTreePtr plan_segment_tree_ptr;

    std::shared_ptr<std::vector<String>> segment_profiles;

    Block block;
    PreparedParameterBindings auto_prepared_params;
};

/**
 * Convert PlanNode to QueryPlan::Node.
 */
class PlanNodeToNodeVisitor : public PlanNodeVisitor<QueryPlan::Node *, Void>
{
public:
    static QueryPlan convert(QueryPlan &);
    explicit PlanNodeToNodeVisitor(QueryPlan & plan_) : plan(plan_) { }
    QueryPlan::Node * visitPlanNode(PlanNodeBase & node, Void & c) override;

private:
    QueryPlan & plan;
};

struct ClusterInfoContext
{
    QueryPlan & query_plan;
    ContextMutablePtr context;
    PlanSegmentTreePtr & plan_segment_tree;
};

class ClusterInfoFinder : public PlanNodeVisitor<std::optional<PlanSegmentContext>, ClusterInfoContext>
{
public:
    static PlanSegmentContext find(QueryPlan & plan, ClusterInfoContext & cluster_info_context);
    explicit ClusterInfoFinder(CTEInfo & cte_info_) : cte_helper(cte_info_) { }
    std::optional<PlanSegmentContext> visitPlanNode(PlanNodeBase & node, ClusterInfoContext & cluster_info_context) override;
    std::optional<PlanSegmentContext> visitTableScanNode(TableScanNode & node, ClusterInfoContext & cluster_info_context) override;
    std::optional<PlanSegmentContext> visitTableWriteNode(TableWriteNode & node, ClusterInfoContext & cluster_info_context) override;
    std::optional<PlanSegmentContext> visitCTERefNode(CTERefNode & node, ClusterInfoContext & cluster_info_context) override;
private:
    SimpleCTEVisitHelper<std::optional<PlanSegmentContext>> cte_helper;
};

class ExplainAnalyzeVisitor : public NodeVisitor<void, PlanSegmentTree::Nodes>
{
public:
    void visitExplainAnalyzeNode(QueryPlan::Node * node, PlanSegmentTree::Nodes &) override;
    void visitNode(QueryPlan::Node * node, PlanSegmentTree::Nodes &) override;
};
}
