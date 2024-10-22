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

#include <sstream>
#include <utility>
#include <IO/Operators.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/PlanVisitor.h>
#include <Interpreters/ProcessorProfile.h>
#include "QueryPlan/PlanNode.h"

namespace DB
{
using PlanNodeId = UInt32;

struct PrinterContext;

class PlanNodePrinter : public PlanNodeVisitor<Void, PrinterContext>
{
public:
    explicit PlanNodePrinter(
        std::stringstream & out_,
        bool with_id_ = false,
        CTEInfo * cte_info = nullptr,
        PlanCostMap plan_cost_map_ = {},
        StepProfiles profiles_ = {})
        : out(out_)
        , cte_helper(cte_info ? std::make_optional<SimpleCTEVisitHelper<void>>(*cte_info) : std::nullopt)
        , with_id(with_id_)
        , plan_cost_map(std::move(plan_cost_map_))
        , profiles(profiles_)
    {
    }

    ~PlanNodePrinter() override = default;
    Void visitPlanNode(PlanNodeBase &, PrinterContext &) override;
    Void visitProjectionNode(ProjectionNode & node, PrinterContext & context) override;
    Void visitExpandNode(ExpandNode & node, PrinterContext & context) override;
    Void visitFilterNode(FilterNode & node, PrinterContext & context) override;
    Void visitJoinNode(JoinNode & node, PrinterContext & context) override;
    Void visitArrayJoinNode(ArrayJoinNode & node, PrinterContext & context) override;
    Void visitAggregatingNode(AggregatingNode & node, PrinterContext & context) override;
    Void visitMarkDistinctNode(MarkDistinctNode & node, PrinterContext & context) override;
    Void visitMergingAggregatedNode(MergingAggregatedNode & node, PrinterContext & context) override;
    Void visitUnionNode(UnionNode & node, PrinterContext & context) override;
    Void visitIntersectNode(IntersectNode & node, PrinterContext & context) override;
    Void visitExceptNode(ExceptNode & node, PrinterContext & context) override;
    Void visitExchangeNode(ExchangeNode & node, PrinterContext & context) override;
    Void visitRemoteExchangeSourceNode(RemoteExchangeSourceNode & node, PrinterContext & context) override;
    Void visitTableScanNode(TableScanNode & node, PrinterContext & context) override;
    Void visitTableWriteNode(TableWriteNode & node, PrinterContext & context) override;
    Void visitTableFinishNode(TableFinishNode & node, PrinterContext & context) override;
    Void visitOutfileWriteNode(OutfileWriteNode & node, PrinterContext & context) override;
    Void visitOutfileFinishNode(OutfileFinishNode & node, PrinterContext & context) override;
    Void visitReadNothingNode(ReadNothingNode & node, PrinterContext & context) override;
    Void visitReadStorageRowCountNode(ReadStorageRowCountNode & node, PrinterContext & context) override;
    Void visitValuesNode(ValuesNode & node, PrinterContext & context) override;
    Void visitLimitNode(LimitNode & node, PrinterContext & context) override;
    Void visitOffsetNode(OffsetNode & node, PrinterContext & context) override;
    Void visitLimitByNode(LimitByNode & node, PrinterContext & context) override;
    Void visitSortingNode(SortingNode & node, PrinterContext & context) override;
    Void visitMergeSortingNode(MergeSortingNode & node, PrinterContext & context) override;
    Void visitPartialSortingNode(PartialSortingNode & node, PrinterContext & context) override;
    Void visitMergingSortedNode(MergingSortedNode & node, PrinterContext & context) override;
    Void visitDistinctNode(DistinctNode & node, PrinterContext & context) override;
    Void visitExtremesNode(ExtremesNode & node, PrinterContext & context) override;
    Void visitTotalsHavingNode(TotalsHavingNode & node, PrinterContext & context) override;
    Void visitFinalSampleNode(FinalSampleNode & node, PrinterContext & context) override;
    Void visitApplyNode(ApplyNode & node, PrinterContext & context) override;
    Void visitEnforceSingleRowNode(EnforceSingleRowNode & node, PrinterContext & context) override;
    Void visitAssignUniqueIdNode(AssignUniqueIdNode & node, PrinterContext & context) override;
    Void visitWindowNode(WindowNode & node, PrinterContext & context) override;
    Void visitCTERefNode(CTERefNode & node, PrinterContext & context) override;
    Void visitPartitionTopNNode(PartitionTopNNode & node, PrinterContext & context) override;
    Void visitExplainAnalyzeNode(ExplainAnalyzeNode & node, PrinterContext & context) override;
    Void visitTopNFilteringNode(TopNFilteringNode & node, PrinterContext & context) override;
    Void visitFillingNode(FillingNode & node, PrinterContext & context) override;
    Void visitIntersectOrExceptNode(IntersectOrExceptNode & node, PrinterContext & context) override;
    Void visitIntermediateResultCacheNode(IntermediateResultCacheNode & node, PrinterContext & context) override;

private:
    void printCTEDefNode(CTEId cte_id);
    std::stringstream & out;
    std::optional<SimpleCTEVisitHelper<void>> cte_helper;
    bool with_id;
    PlanCostMap plan_cost_map;
    StepProfiles profiles;
    void printNode(const PlanNodeBase & node, const String & label, const String & details, const String & color, PrinterContext & context);
    Void visitChildren(PlanNodeBase &, PrinterContext &);
    void printHints(const PlanNodeBase & node);
};

class PlanNodeEdgePrinter : public PlanNodeVisitor<Void, Void>
{
public:
    explicit PlanNodeEdgePrinter(std::stringstream & out_, CTEInfo * cte_info = nullptr)
        : out(out_), cte_helper(cte_info ? std::make_optional<SimpleCTEVisitHelper<void>>(*cte_info) : std::nullopt)
    {
    }
    Void visitPlanNode(PlanNodeBase &, Void &) override;
    Void visitCTERefNode(CTERefNode & node, Void & c) override;
    Void visitJoinNode(JoinNode & node, Void & c) override;

private:
    std::stringstream & out;
    std::optional<SimpleCTEVisitHelper<void>> cte_helper;
    void printEdge(PlanNodeBase & from, PlanNodeBase & to, std::string_view format = "");
};

class PlanSegmentNodePrinter : public NodeVisitor<Void, PrinterContext>
{
public:
    explicit PlanSegmentNodePrinter(std::stringstream & out_, bool with_id_ = false) : out(out_), with_id(with_id_) { }
    ~PlanSegmentNodePrinter() override = default;
    Void visitNode(QueryPlan::Node *, PrinterContext &) override;
    Void visitProjectionNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExpandNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitFilterNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitJoinNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitArrayJoinNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitAggregatingNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitMarkDistinctNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitMergingAggregatedNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitUnionNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitIntersectNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExceptNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExchangeNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitRemoteExchangeSourceNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitTableScanNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitTableWriteNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitTableFinishNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitOutfileWriteNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitOutfileFinishNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitReadNothingNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitReadStorageRowCountNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitValuesNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitLimitNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitOffsetNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitLimitByNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitSortingNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitMergeSortingNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitPartialSortingNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitMergingSortedNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitDistinctNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExtremesNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitTotalsHavingNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitFinalSampleNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitApplyNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitEnforceSingleRowNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitAssignUniqueIdNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitWindowNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitPartitionTopNNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitExplainAnalyzeNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitTopNFilteringNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitFillingNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitIntersectOrExceptNode(QueryPlan::Node * node, PrinterContext & context) override;
    Void visitIntermediateResultCacheNode(QueryPlan::Node * node, PrinterContext & context) override;

private:
    std::stringstream & out;
    bool with_id;
    void printNode(QueryPlan::Node * node, const String & label, const String & details, const String & color, PrinterContext & context);
    Void visitChildren(QueryPlan::Node *, PrinterContext &);
};

class PlanSegmentEdgePrinter : public NodeVisitor<Void, std::unordered_map<size_t, PlanSegmentPtr &>>
{
public:
    explicit PlanSegmentEdgePrinter(std::stringstream & out_) : out(out_) { }
    Void visitNode(QueryPlan::Node *, std::unordered_map<size_t, PlanSegmentPtr &> &) override;
    Void visitRemoteExchangeSourceNode(QueryPlan::Node * node, std::unordered_map<size_t, PlanSegmentPtr &> &) override;

private:
    std::stringstream & out;
    void printEdge(QueryPlan::Node * from, QueryPlan::Node * to);
};

class StepPrinter
{
public:
    static String printStep(const IQueryPlanStep & step, bool include_output = true);
    static String printProjectionStep(const ProjectionStep & step, bool include_output = true);
    static String printExpandStep(const ExpandStep & step, bool include_output = true);
    static String printFilterStep(const FilterStep & step, bool include_output = true);
    static String printJoinStep(const JoinStep & step);
    static String printArrayJoinStep(const ArrayJoinStep & step);
    static String printAggregatingStep(const AggregatingStep & step, bool include_output = true);
    static String printMarkDistinctStep(const MarkDistinctStep & step, bool include_output = true);
    static String printMergingAggregatedStep(const MergingAggregatedStep & step);
    static String printUnionStep(const UnionStep & step);
    static String printIntersectStep(const IntersectStep & step);
    static String printExceptStep(const ExceptStep & step);
    static String printExchangeStep(const ExchangeStep & step);
    static String printRemoteExchangeSourceStep(const RemoteExchangeSourceStep & step);
    static String printFinalSampleStep(const FinalSampleStep & step);
    static String printTableScanStep(const TableScanStep & step);
    static String printTableWriteStep(const TableWriteStep & step);
    static String printTableFinishStep(const TableFinishStep & step);
    static String printOutfileWriteStep(const OutfileWriteStep & step);
    static String printOutfileFinishStep(const OutfileFinishStep & step);
    static String printReadStorageRowCountStep(const ReadStorageRowCountStep & step);
    static String printValuesStep(const ValuesStep & step);
    static String printLimitStep(const LimitStep & step);
    static String printOffsetStep(const OffsetStep & step);
    static String printLimitByStep(const LimitByStep & step);
    static String printSortingStep(const SortingStep & step);
    static String printMergeSortingStep(const MergeSortingStep & step);
    static String printPartialSortingStep(const PartialSortingStep & step);
    static String printMergingSortedStep(const MergingSortedStep & step);
    static String printDistinctStep(const DistinctStep & step);
    static String printApplyStep(const ApplyStep & step);
    static String printEnforceSingleRowStep(const EnforceSingleRowStep & step);
    static String printAssignUniqueIdStep(const AssignUniqueIdStep & step);
    static String printWindowStep(const WindowStep & step);
    static String printCTERefStep(const CTERefStep & step);
    static String printPartitionTopNStep(const PartitionTopNStep & step);
    static String printExplainAnalyzeStep(const ExplainAnalyzeStep & step);
    static String printTopNFilteringStep(const TopNFilteringStep & step);
    static String printFillingStep(const FillingStep & step);
    static String printIntersectOrExceptStep(const IntersectOrExceptStep & step);
    static String printTotalsHavingStep(const TotalsHavingStep & step);
    static String printExtremesStep(const ExtremesStep & step);
    static String printIntermediateResultCacheStep(const IntermediateResultCacheStep & step);

private:
    static String printFilter(const ConstASTPtr & filter);
};

class GraphvizPrinter
{
public:
    const static int PRINT_AST_INDEX = 1000;
    const static int PRINT_PLAN_BUILD_INDEX = 2000;
    const static int PRINT_PLAN_OPTIMIZE_INDEX = 3000;
    const static String MEMO_GRAPH_PATH;
    const static String PIPELINE_PATH;

    static void printAST(const ASTPtr &, ContextMutablePtr & context, const String & visitor);
    static void printLogicalPlan(PlanNodeBase &, ContextMutablePtr &, const String & name);
    static void printLogicalPlan(QueryPlan &, ContextMutablePtr &, const String & name, StepProfiles profiles = {});
    static void printMemo(const Memo & memo, const ContextMutablePtr & context, const String & name);
    static void printMemo(const Memo & memo, GroupId root_id, const ContextMutablePtr & context, const String & name);
    static void printPlanSegment(const PlanSegmentTreePtr &, const ContextMutablePtr &);
    static void printChunk(String transform, const Block & block, const Chunk & chunk);
    static void printPipeline(const Processors & processors, const ExecutingGraphPtr & graph, const ContextPtr & context, size_t segment_id, const String & host);
    static String getColor(IQueryPlanStep::Type step);
    static String printSettings(const String & color, const ContextMutablePtr & context);

private:
    static String printAST(ASTPtr);
    static void addID(ASTPtr & ast, std::unordered_map<ASTPtr, UInt16> & asts, std::shared_ptr<std::atomic<UInt16>> & max_node_id);

    static String printLogicalPlan(PlanNodeBase &, CTEInfo * cte_info = nullptr, StepProfiles profiles = {});
    static String printPlanSegmentNodes(const PlanSegmentTreePtr &, const ContextMutablePtr &);
    static void appendPlanSegmentNodes(
        std::stringstream & out,
        PlanSegmentTree::Node * segmentNode,
        std::unordered_map<size_t, PlanSegmentPtr &> &,
        std::unordered_set<PlanSegmentTree::Node *> & visited);
    static void appendPlanSegmentNode(std::stringstream & out, const PlanSegmentPtr & segment_ptr);

    static String printMemo(const Memo & memo, GroupId root_id);
    static String printGroup(const Group & group, const std::unordered_map<GroupId, WinnerPtr> & group_winner);

    static String printPipeline(const Processors & processors, const ExecutingGraphPtr & graph);
    static String printGroupedPipeline(const Processors & processors, const ExecutingGraphPtr & graph);
};

}
