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

#include <Optimizer/PlanOptimizer.h>

#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Iterative/IterativeRewriter.h>
#include <Optimizer/PlanCheck.h>
#include <Optimizer/Rewriter/AddBufferForDeadlockCTE.h>
#include <Optimizer/Rewriter/AddExchange.h>
#include <Optimizer/Rewriter/AddRuntimeFilters.h>
#include <Optimizer/Rewriter/BitmapIndexSplitter.h>
#include <Optimizer/Rewriter/ColumnPruning.h>
#include <Optimizer/Rewriter/EliminateJoinByForeignKey.h>
#include <Optimizer/Rewriter/GroupByKeysPruning.h>
#include <Optimizer/Rewriter/MaterializedViewRewriter.h>
#include <Optimizer/Rewriter/OptimizeTrivialCount.h>
#include <Optimizer/Rewriter/PredicatePushdown.h>
#include <Optimizer/Rewriter/RemoveApply.h>
#include <Optimizer/Rewriter/RemoveRedundantAggregate.h>
#include <Optimizer/Rewriter/RemoveRedundantSort.h>
#include <Optimizer/Rewriter/RemoveUnusedCTE.h>
#include <Optimizer/Rewriter/ShareCommonExpression.h>
#include <Optimizer/Rewriter/SimpleReorderJoin.h>
#include <Optimizer/Rewriter/SimplifyCrossJoin.h>
#include <Optimizer/Rewriter/UnifyJoinOutputs.h>
#include <Optimizer/Rewriter/UnifyNullableType.h>
#include <Optimizer/Rewriter/UseSortingProperty.h>
#include <Optimizer/Rule/Rules.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/Hints/HintsPropagator.h>
#include <QueryPlan/Hints/ImplementJoinAlgorithmHints.h>
#include <QueryPlan/Hints/ImplementJoinOperationHints.h>
#include <QueryPlan/Hints/ImplementJoinOrderHints.h>
#include <QueryPlan/PlanPattern.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OPTIMIZER_NONSUPPORT;
    extern const int OPTIMIZER_TIMEOUT;
}

const Rewriters & PlanOptimizer::getSimpleRewriters()
{
    static Rewriters simple_rewrites = {
        // rules for normalize plan (DO NOT change !!!)
        std::make_shared<HintsPropagator>(),
        std::make_shared<ColumnPruning>(),
        std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), "NormalizeExpression"),
        std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
        std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),

        // rules for normalize Union/Except/Intersect
        std::make_shared<IterativeRewriter>(Rules::mergeSetRules(), "MergeSetNode"),

        std::make_shared<BitmapIndexSplitter>(),
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
        std::make_shared<IterativeRewriter>(Rules::pushDownBitmapProjection(), "PushDownBitmapProjection"),
        std::make_shared<ColumnPruning>(),
        std::make_shared<IterativeRewriter>(Rules::pushIndexProjectionIntoTableScanRules(), "PushIndexProjectionIntoTableScan"),

        std::make_shared<ColumnPruning>(),
        std::make_shared<RemoveRedundantDistinct>(),

        std::make_shared<RemoveRedundantSort>(),
        std::make_shared<PredicatePushdown>(),

        // normalize plan after predicate push down
        std::make_shared<ColumnPruning>(),
        std::make_shared<RemoveRedundantDistinct>(),
        std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
        std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),

        //add reorder adjacent windows
        std::make_shared<IterativeRewriter>(Rules::swapAdjacentRules(), "SwapAdjacent"),
        std::make_shared<ImplementJoinOrderHints>(),

        // push down limit and aggregate
        std::make_shared<IterativeRewriter>(Rules::pushDownLimitRules(), "PushDownLimit"),
        std::make_shared<IterativeRewriter>(Rules::distinctToAggregateRules(), "DistinctToAggregate"),
        std::make_shared<ColumnPruning>(false, true),

        std::make_shared<ImplementJoinOrderHints>(),

        std::make_shared<MaterializedViewRewriter>(),
        std::make_shared<ImplementJoinOperationHints>(),

        /// topn filtering optimization
        /// rules use novel operators should be placed after MaterializedViewRewriter, in case of MV matching failure
        std::make_shared<IterativeRewriter>(Rules::pushDownTopNRules(), "pushDownTopNRules"),
        std::make_shared<IterativeRewriter>(Rules::createTopNFilteringRules(), "createTopNFiltering"),
        std::make_shared<IterativeRewriter>(Rules::pushDownTopNFilteringRules(), "pushDownTopNFiltering"),

        // add exchange
        std::make_shared<CascadesOptimizer>(false),

        std::make_shared<IterativeRewriter>(Rules::pushPartialStepRules(), "PushPartialStep"),
        std::make_shared<IterativeRewriter>(Rules::optimizeAggregateRules(), "OptimizeAggregate"),
        std::make_shared<RemoveRedundantDistinct>(),

        // use property
        std::make_shared<SortingOrderedSource>(),

        std::make_shared<OptimizeTrivialCount>(),
        std::make_shared<IterativeRewriter>(Rules::pushIntoTableScanRules(), "PushIntoTableScan"),
        std::make_shared<ShareCommonExpression>(), // this rule depends on enable_optimizer_early_prewhere_push_down
        std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
        // column pruned by add extra projection, DO NOT ADD RemoveRedundant rule after this rule !!!
        std::make_shared<ColumnPruning>(true),
        std::make_shared<UnifyNullableType>(), /* some rules generates incorrect column ptr for DataStream,
                                                  e.g. use a non-nullable column ptr for a nullable column */
        std::make_shared<AddBufferForDeadlockCTE>(),
        std::make_shared<IterativeRewriter>(Rules::pushTableScanEmbeddedStepRules(), "PushTableScanEmbeddedStepRules"),
        std::make_shared<IterativeRewriter>(Rules::explainAnalyzeRules(), "ExplainAnalyze"),
    };
    return simple_rewrites;
}

const Rewriters & PlanOptimizer::getFullRewriters()
{
    // the order of rules matters, DO NOT change.
    static Rewriters full_rewrites = {
        std::make_shared<HintsPropagator>(),
        std::make_shared<ColumnPruning>(),
        std::make_shared<UnifyNullableType>(),

        // remove subquery rely on specific pattern
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),

        // when correlated-subquery exists, we can't perform simplify expression actions, because
        // type analyzer relay on input columns to resolve the data type of identifiers. for correlated
        // symbols, it's unknown. after subquery removed, simplify expression is able to execute.
        // Normalize expression, like, common predicate rewrite, swap predicate rewrite, these rules
        // they don't need type analyzer.
        // Simplify expression, like, expression interpret, unwrap cast. these rules require type analyzer.
        std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), "NormalizeExpression"),
        std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
        // removeRedundantRules may remove cte, so we need to remove unused cte after RemoveRedundant.
        std::make_shared<RemoveUnusedCTE>(),

        // rules for normalize Union/Except/Intersect
        std::make_shared<IterativeRewriter>(Rules::mergeSetRules(), "MergeSetNode"),
        std::make_shared<IterativeRewriter>(Rules::implementSetRules(), "ImplementSetNode"),

        // rules for remove subquery, the order of subquery rules matters, DO NOT change !!!.
        std::make_shared<IterativeRewriter>(Rules::pushApplyRules(), "PushApply"),
        std::make_shared<RemoveUnCorrelatedInSubquery>(),
        std::make_shared<RemoveCorrelatedInSubquery>(),
        std::make_shared<RemoveUnCorrelatedExistsSubquery>(),
        std::make_shared<RemoveCorrelatedExistsSubquery>(),
        std::make_shared<RemoveUnCorrelatedScalarSubquery>(),
        std::make_shared<RemoveCorrelatedScalarSubquery>(),
        std::make_shared<RemoveUnCorrelatedQuantifiedComparisonSubquery>(),
        std::make_shared<RemoveCorrelatedQuantifiedComparisonSubquery>(),

        std::make_shared<BitmapIndexSplitter>(),
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
        std::make_shared<IterativeRewriter>(Rules::pushDownBitmapProjection(), "PushDownBitMapProjection"),
        std::make_shared<ColumnPruning>(),

        // rules after subquery removed, DO NOT change !!!.

        std::make_shared<RemoveRedundantSort>(),

        // subquery remove may generate outer join, make sure data type is correct.
        std::make_shared<ColumnPruning>(),
        std::make_shared<UnifyNullableType>(),

        // predicate push down
        std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
        std::make_shared<PredicatePushdown>(true),

        // predicate push down may convert outer-join to inner-join, make sure data type is correct.
        std::make_shared<ColumnPruning>(),
        std::make_shared<RemoveRedundantDistinct>(),
        std::make_shared<UnifyNullableType>(),

        // Join graph requires projection inline/merge/pull up, as projection will break join graph.
        // Join graph will pull up predicates, hence, apply predicate push down after it.

        // simplify cross join
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
        std::make_shared<SimplifyCrossJoin>(),
        std::make_shared<PredicatePushdown>(),
        // predicate push down may convert outer-join to inner-join, make sure data type is correct.
        std::make_shared<ColumnPruning>(),
        std::make_shared<UnifyNullableType>(),

        // simple join order (primary for large joins reorder)
        std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
        std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
        std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), "NormalizeExpression"),
        std::make_shared<IterativeRewriter>(Rules::swapPredicateRules(), "SwapPredicate"),

        // push down limit & aggregate
        std::make_shared<IterativeRewriter>(Rules::pushDownLimitRules(), "PushDownLimit"),
        std::make_shared<IterativeRewriter>(Rules::distinctToAggregateRules(), "DistinctToAggregate"),
        std::make_shared<ColumnPruning>(false, true),
        std::make_shared<IterativeRewriter>(Rules::pushAggRules(), "PushAggregateThroughJoin"),

        std::make_shared<ImplementJoinOrderHints>(),

        std::make_shared<GroupByKeysPruning>(),
        std::make_shared<EliminateJoinByFK>(),

        std::make_shared<SimpleReorderJoin>(),
        std::make_shared<PredicatePushdown>(),

        // predicate push down may convert outer-join to inner-join, make sure data type is correct.
        std::make_shared<ColumnPruning>(),
        std::make_shared<RemoveRedundantDistinct>(),
        std::make_shared<UnifyNullableType>(),

        // prepare for cascades
        std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
        std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
        std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), "NormalizeExpression"),
        std::make_shared<UnifyJoinOutputs>(),

        // remove unused CTE before cascades
        std::make_shared<RemoveUnusedCTE>(),

        //add reorder adjacent windows
        std::make_shared<IterativeRewriter>(Rules::swapAdjacentRules(), "SwapAdjacent"),

        //
        std::make_shared<MaterializedViewRewriter>(),
        std::make_shared<ImplementJoinOperationHints>(),

        /// topn filtering optimization
        /// rules use novel operators should be placed after MaterializedViewRewriter, in case of MV matching failure
        std::make_shared<IterativeRewriter>(Rules::pushDownTopNRules(), "pushDownTopNRules"),
        std::make_shared<IterativeRewriter>(Rules::createTopNFilteringRules(), "createTopNFiltering"),
        std::make_shared<IterativeRewriter>(Rules::pushDownTopNFilteringRules(), "pushDownTopNFiltering"),

        // Cost-based optimizer
        std::make_shared<CascadesOptimizer>(),

        // remove not inlined CTEs
        std::make_shared<RemoveUnusedCTE>(),

        // add runtime filters
        std::make_shared<AddRuntimeFilters>(),

        // final UnifyNullableType, make sure type is correct.
        std::make_shared<ColumnPruning>(),
        std::make_shared<UnifyNullableType>(),
        /// Predicate pushdown (AddRuntimeFilters) may generate redundant filter.
        std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
        std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),

        // push partial step through exchange
        // TODO cost-base partial aggregate push down
        std::make_shared<IterativeRewriter>(Rules::pushPartialStepRules(), "PushPartialStep"),
        std::make_shared<IterativeRewriter>(Rules::optimizeAggregateRules(), "OptimizeAggregate"),
        std::make_shared<RemoveRedundantDistinct>(),

        // use property
        std::make_shared<SortingOrderedSource>(),

        std::make_shared<OptimizeTrivialCount>(),
        // push predicate into storage
        std::make_shared<IterativeRewriter>(Rules::pushIntoTableScanRules(), "PushIntoTableScan"),
        std::make_shared<ShareCommonExpression>(), // this rule depends on enable_optimizer_early_prewhere_push_down
        // TODO cost-based projection push down
        std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
        std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
        // column pruned by add extra projection, DO NOT ADD RemoveRedundant rule after this rule !!!        
        std::make_shared<ColumnPruning>(true),
        std::make_shared<UnifyNullableType>(), /* some rules generates incorrect column ptr for DataStream,
                                                  e.g. use a non-nullable column ptr for a nullable column */
        std::make_shared<AddBufferForDeadlockCTE>(),
        std::make_shared<IterativeRewriter>(Rules::pushTableScanEmbeddedStepRules(), "PushTableScanEmbeddedStepRules"),
        std::make_shared<ImplementJoinAlgorithmHints>(),
        std::make_shared<IterativeRewriter>(Rules::explainAnalyzeRules(), "ExplainAnalyze"),
    };

    return full_rewrites;
}

void PlanOptimizer::optimize(QueryPlan & plan, ContextMutablePtr context)
{
    int i = GraphvizPrinter::PRINT_PLAN_OPTIMIZE_INDEX;
    GraphvizPrinter::printLogicalPlan(plan, context, std::to_string(i++) + "_Init_Plan");

    // Check init plan to satisfy with :
    // 1 Symbol exist check
    PlanCheck::checkInitPlan(plan, context);
    Stopwatch rule_watch, total_watch;
    total_watch.start();

    if (PlanPattern::isSimpleQuery(plan))
    {
        optimize(plan, context, getSimpleRewriters());
    }
    else
    {
        optimize(plan, context, getFullRewriters());
    }

    // Check final plan to satisfy with :
    // 1 Symbol exist check
    total_watch.restart();
    PlanCheck::checkFinalPlan(plan, context);

    context->logOptimizerProfile(&Poco::Logger::get("PlanOptimizer"),
                                "Optimizer stage run time: ",
                                "checkFinalPlan",
                                std::to_string(total_watch.elapsedMillisecondsAsDouble()) + "ms", true);
}

void PlanOptimizer::optimize(QueryPlan & plan, ContextMutablePtr context, const Rewriters & rewriters)
{
    context->setRuleId(GraphvizPrinter::PRINT_PLAN_OPTIMIZE_INDEX);

    Stopwatch total_watch{CLOCK_THREAD_CPUTIME_ID};
    total_watch.start();

    for (const auto & rewriter : rewriters)
    {
        context->incRuleId();
        rewriter->rewritePlan(plan, context);
        UInt64 elapsed = total_watch.elapsedMilliseconds();

        if (elapsed >= context->getSettingsRef().plan_optimizer_timeout)
        {
            throw Exception(
                "PlanOptimizer exhausted the time limit of " + std::to_string(context->getSettingsRef().plan_optimizer_timeout) + " ms",
                ErrorCodes::OPTIMIZER_TIMEOUT);
        }
    }

    UInt64 elapsed = total_watch.elapsedMilliseconds();
    LOG_DEBUG(&Poco::Logger::get("PlanOptimizer"), "Total optimizer time: " + std::to_string(elapsed));
}
}
