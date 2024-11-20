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
#include <optional>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Optimizer/Property/Property.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>


namespace DB
{
using PlanSegmentResult = QueryPlan::Node *;

class PlanSegmentInput;
using PlanSegmentInputPtr = std::shared_ptr<PlanSegmentInput>;
using PlanSegmentInputs = std::vector<PlanSegmentInputPtr>;
struct PlanSegmentContext;

class PlanSegmentSplitter
{
public:
    static void split(QueryPlan & query_plan, PlanSegmentContext & plan_segment_context);
};

struct PlanSegmentContext
{
    ContextMutablePtr context;
    QueryPlan & query_plan;
    String query_id;
    size_t id = 0;
    size_t shard_number = 0;
    String cluster_name;
    PlanSegmentTree * plan_segment_tree;
    size_t getSegmentId() { return id++; }
};

struct PlanSegmentVisitorContext
{
    PlanSegmentInputs inputs;
    std::vector<PlanSegment *> children;
    size_t & exchange_id;
    String hash_func;
    Array params = Array();
    bool is_add_totals = false;
    bool is_add_extremes = false;
    bool scalable = true;
};

class PlanSegmentVisitor : public NodeVisitor<PlanSegmentResult, PlanSegmentVisitorContext>
{
public:
    explicit PlanSegmentVisitor(PlanSegmentContext & plan_segment_context_, QueryPlan::CTENodes & cte_nodes_)
        : plan_segment_context(plan_segment_context_), cte_nodes(cte_nodes_)
    {
    }

    PlanSegmentResult visitNode(QueryPlan::Node *, PlanSegmentVisitorContext & split_context) override;
    PlanSegmentResult visitExchangeNode(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context) override;
    PlanSegmentResult visitCTERefNode(QueryPlan::Node * node, PlanSegmentVisitorContext & context) override;
    PlanSegmentResult visitTotalsHavingNode(QueryPlan::Node * node, PlanSegmentVisitorContext & context) override;
    PlanSegmentResult visitExtremesNode(QueryPlan::Node * node, PlanSegmentVisitorContext & context) override;

    PlanSegment * createPlanSegment(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context);
    PlanSegment * createPlanSegment(QueryPlan::Node * node, size_t segment_id, PlanSegmentVisitorContext & split_context);

private:
    PlanSegmentResult visitChild(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context);
    PlanSegmentInputs findInputs(QueryPlan::Node * node);
    std::pair<String, size_t> findClusterAndParallelSize(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context);

    PlanSegmentContext & plan_segment_context;
    QueryPlan::CTENodes & cte_nodes;
    std::unordered_map<CTEId, std::pair<PlanSegment *, ExchangeStep *>> cte_plan_segments{};
};

class SourceNodeFinder : public NodeVisitor<std::vector<std::optional<Partitioning::Handle>>, const Context>
{
public:
    explicit SourceNodeFinder(QueryPlan::CTENodes & cte_nodes_) : cte_nodes(cte_nodes_) { }
    static std::vector<Partitioning::Handle> find(QueryPlan::Node * node, QueryPlan::CTENodes & cte_nodes, const Context & context);

    std::vector<std::optional<Partitioning::Handle>> visitNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<std::optional<Partitioning::Handle>> visitValuesNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<std::optional<Partitioning::Handle>> visitReadNothingNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<std::optional<Partitioning::Handle>> visitTableScanNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<std::optional<Partitioning::Handle>> visitRemoteExchangeSourceNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<std::optional<Partitioning::Handle>> visitExchangeNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<std::optional<Partitioning::Handle>> visitCTERefNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<std::optional<Partitioning::Handle>> visitReadStorageRowCountNode(QueryPlan::Node * node, const Context & context) override;

private:
    QueryPlan::CTENodes & cte_nodes;
};

class SetScalable : public NodeVisitor<Void, const Context>
{
    SetScalable(bool scalable_, QueryPlan::CTENodes & cte_nodes_) : scalable(scalable_), cte_nodes(cte_nodes_) { }

public:
    static void setScalable(QueryPlan::Node * node, QueryPlan::CTENodes & cte_nodes, const Context & context);

    Void visitNode(QueryPlan::Node * node, const Context & context) override;
    Void visitExchangeNode(QueryPlan::Node * node, const Context & context) override;
    Void visitCTERefNode(QueryPlan::Node * node, const Context & context) override;

private:
    bool scalable;
    QueryPlan::CTENodes & cte_nodes;
};

class ParallelSizeChecker: public NodeVisitor<std::vector<size_t>, const Context>
{
public:
    std::vector<size_t> visitNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<size_t> visitValuesNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<size_t> visitReadNothingNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<size_t> visitTableScanNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<size_t> visitRemoteExchangeSourceNode(QueryPlan::Node * node, const Context & context) override;
    std::vector<size_t> visitReadStorageRowCountNode(QueryPlan::Node * node, const Context & context) override;

    PlanSegment * segment;
    std::vector<PlanSegment *> children_segments;
    size_t shard_number;
};

}
