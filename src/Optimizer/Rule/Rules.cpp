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

#include <memory>
#include <Optimizer/Rule/Rules.h>

#include <Optimizer/Rewriter/RemoveApply.h>
#include <Optimizer/Rule/Rewrite/CrossJoinToUnion.h>
#include <Optimizer/Rule/Rewrite/AddRepartitionColumn.h>
#include <Optimizer/Rule/Rewrite/DistinctToAggregate.h>
#include <Optimizer/Rule/Rewrite/EagerAggregation.h>
#include <Optimizer/Rule/Rewrite/ExplainAnalyzeRules.h>
#include <Optimizer/Rule/Rewrite/ExtractBitmapImplicitFilter.h>
#include <Optimizer/Rule/Rewrite/FilterWindowToPartitionTopN.h>
#include <Optimizer/Rule/Rewrite/ImplementSetOperationRules.h>
#include <Optimizer/Rule/Rewrite/InlineProjections.h>
#include <Optimizer/Rule/Rewrite/MergeSetOperationRules.h>
#include <Optimizer/Rule/Rewrite/MultipleDistinctAggregationToExpandAggregate.h>
#include <Optimizer/Rule/Rewrite/MultipleDistinctAggregationToMarkDistinct.h>
#include <Optimizer/Rule/Rewrite/OptimizeAggregateRules.h>
#include <Optimizer/Rule/Rewrite/PullProjectionOnJoinThroughJoin.h>
#include <Optimizer/Rule/Rewrite/PushAggThroughJoinRules.h>
#include <Optimizer/Rule/Rewrite/PushDownApplyRules.h>
#include <Optimizer/Rule/Rewrite/PushDownLimitRules.h>
#include <Optimizer/Rule/Rewrite/PushIntoTableScanRules.h>
#include <Optimizer/Rule/Rewrite/PushPartialStepThroughExchangeRules.h>
#include <Optimizer/Rule/Rewrite/PushProjectionRules.h>
#include <Optimizer/Rule/Rewrite/PushThroughExchangeRules.h>
#include <Optimizer/Rule/Rewrite/PushUnionThroughJoin.h>
#include <Optimizer/Rule/Rewrite/RemoveRedundantRules.h>
#include <Optimizer/Rule/Rewrite/SimplifyExpressionRules.h>
#include <Optimizer/Rule/Rewrite/SingleDistinctAggregationToGroupBy.h>
#include <Optimizer/Rule/Rewrite/SumIfToCountIf.h>
#include <Optimizer/Rule/Rewrite/SwapAdjacentRules.h>
#include <Optimizer/Rule/Rewrite/TopNRules.h>
#include <Optimizer/Rule/Rewrite/JoinUsingToJoinOn.h>

namespace DB
{
std::vector<RulePtr> Rules::mergeSetRules()
{
    return {std::make_shared<MergeUnionRule>(), std::make_shared<MergeIntersectRule>(), std::make_shared<MergeExceptRule>()};
}

std::vector<RulePtr> Rules::implementSetRules()
{
    return {std::make_shared<ImplementIntersectRule>(), std::make_shared<ImplementExceptRule>()};
}

std::vector<RulePtr> Rules::normalizeExpressionRules()
{
    return {std::make_shared<CommonPredicateRewriteRule>(), std::make_shared<CommonJoinFilterRewriteRule>()};
}

std::vector<RulePtr> Rules::swapPredicateRules()
{
    return {std::make_shared<SwapPredicateRewriteRule>()};
}

std::vector<RulePtr> Rules::sumIfToCountIf()
{
    return {std::make_shared<SumIfToCountIf>()};
}

std::vector<RulePtr> Rules::simplifyExpressionRules()
{
    return {
        std::make_shared<SimplifyPredicateRewriteRule>(),
        std::make_shared<UnWarpCastInPredicateRewriteRule>(),
        std::make_shared<SimplifyJoinFilterRewriteRule>(),
        std::make_shared<SimplifyExpressionRewriteRule>(),
        std::make_shared<MergePredicatesUsingDomainTranslator>()};
}

std::vector<RulePtr> Rules::inlineProjectionRules()
{
    // todo@kaixi: remove InlineProjectionIntoJoin
    return {
        std::make_shared<InlineProjectionIntoJoin>(),
        std::make_shared<InlineProjectionOnJoinIntoJoin>(),
        std::make_shared<InlineProjections>(),
        std::make_shared<PullProjectionOnJoinThroughJoin>()};
}

std::vector<RulePtr> Rules::pushPartialStepRules()
{
    return {
        std::make_shared<PushPartialAggThroughExchange>(),
        std::make_shared<PushPartialAggThroughUnion>(),
        std::make_shared<PushPartialSortingThroughExchange>(),
        std::make_shared<PushPartialSortingThroughUnion>(),
        std::make_shared<PushPartialLimitThroughExchange>(),
        std::make_shared<PushProjectionThroughExchange>(),
        std::make_shared<FilterWindowToPartitionTopN>(),
        std::make_shared<PushPartialDistinctThroughExchange>()};
}

std::vector<RulePtr> Rules::optimizeAggregateRules()
{
    return {std::make_shared<OptimizeMemoryEfficientAggregation>()};
}

std::vector<RulePtr> Rules::removeRedundantRules()
{
    return {
        std::make_shared<RemoveRedundantFilter>(),
        std::make_shared<RemoveRedundantProjection>(),
        std::make_shared<RemoveRedundantEnforceSingleRow>(),
        std::make_shared<RemoveRedundantUnion>(),
        std::make_shared<RemoveRedundantCrossJoin>(),
        std::make_shared<RemoveRedundantJoin>(),
        std::make_shared<RemoveRedundantLimit>(),
        // std::make_shared<RemoveRedundantOuterJoin>()
        std::make_shared<RemoveRedundantTwoApply>(),
        std::make_shared<RemoveRedundantAggregateWithReadNothing>(),
    };
}

std::vector<RulePtr> Rules::pushAggRules()
{
    return {std::make_shared<PushAggThroughOuterJoin>(), std::make_shared<EagerAggregation>()};
}

std::vector<RulePtr> Rules::pushDownLimitRules()
{
    return {
        std::make_shared<LimitZeroToReadNothing>(),
        std::make_shared<PushLimitIntoDistinct>(),
        std::make_shared<PushLimitThroughProjection>(),
        std::make_shared<PushLimitThroughExtremesStep>(),
        std::make_shared<PushLimitThroughOuterJoin>(),
        std::make_shared<PushLimitThroughUnion>(),
        std::make_shared<PushdownLimitIntoWindow>(),
        std::make_shared<PushTopNThroughProjection>(),
        std::make_shared<PushSortThroughProjection>(),
        std::make_shared<PushLimitIntoSorting>()};
}

std::vector<RulePtr> Rules::distinctToAggregateRules()
{
    return {
        // std::make_shared<DistinctToAggregate>(),
        std::make_shared<SingleDistinctAggregationToGroupBy>(),
        std::make_shared<MultipleDistinctAggregationToMarkDistinct>(),
        std::make_shared<MultipleDistinctAggregationToExpandAggregate>(),
    };
}

std::vector<RulePtr> Rules::pushIntoTableScanRules()
{
    return {std::make_shared<PushLimitIntoTableScan>(), std::make_shared<PushStorageFilter>()};
}

std::vector<RulePtr> Rules::pushTableScanEmbeddedStepRules()
{
    // enabled when optimizer_projection_support = 1
    return {
        std::make_shared<PushAggregationIntoTableScan>(),
        std::make_shared<PushProjectionIntoTableScan>(),
        std::make_shared<PushFilterIntoTableScan>()};
}

std::vector<RulePtr> Rules::pushDownBitmapProjection()
{
    return {
        std::make_shared<PushProjectionThroughFilter>(),
        std::make_shared<PushProjectionThroughProjection>(),
        std::make_shared<InlineProjections>(true)};
}

std::vector<RulePtr> Rules::pushProjectionIntoTableScanRules()
{
    return {std::make_shared<PushProjectionIntoTableScan>()};
}

std::vector<RulePtr> Rules::pushIndexProjectionIntoTableScanRules()
{
    // enable when optimizer_index_projection_support = 1
    return {std::make_shared<PushIndexProjectionIntoTableScan>()};
}

std::vector<RulePtr> Rules::swapAdjacentRules()
{
    return {std::make_shared<SwapAdjacentWindows>()};
}

std::vector<RulePtr> Rules::explainAnalyzeRules()
{
    return {std::make_shared<ExplainAnalyze>()};
}

std::vector<RulePtr> Rules::pushDownTopNRules()
{
    return {std::make_shared<PushTopNThroughProjection>(), std::make_shared<PushSortThroughProjection>()};
}

std::vector<RulePtr> Rules::createTopNFilteringRules()
{
    return {
        std::make_shared<CreateTopNFilteringForAggregating>(),
        std::make_shared<CreateTopNFilteringForDistinct>(),
        std::make_shared<CreateTopNFilteringForAggregatingLimit>(),
        std::make_shared<CreateTopNFilteringForDistinctLimit>()};
}

std::vector<RulePtr> Rules::pushDownTopNFilteringRules()
{
    /// PushTopNFilteringXXX rules cannot be mixed with CreateTopNFilteringXXX rules,
    /// as create rules will produce redundant TopNFilteringSteps when the last produced one is pushdowned.
    return {std::make_shared<PushTopNFilteringThroughProjection>(), std::make_shared<PushTopNFilteringThroughUnion>()};
}

std::vector<RulePtr> Rules::pushApplyRules()
{
    return {std::make_shared<PushDownApplyThroughJoin>()};
}

std::vector<RulePtr> Rules::unnestingSubqueryRules()
{
    return {
        std::make_shared<UnnestingWithWindow>(),
        std::make_shared<UnnestingWithProjectionWindow>(),
        std::make_shared<ExistsToSemiJoin>(),
        std::make_shared<InToSemiJoin>()};
}

std::vector<RulePtr> Rules::crossJoinToUnion()
{
    return {std::make_shared<CrossJoinToUnion>()};
}

std::vector<RulePtr> Rules::extractBitmapImplicitFilterRules()
{
    return {std::make_shared<ExtractBitmapImplicitFilter>()};
}

std::vector<RulePtr> Rules::pushUnionThroughJoin()
{
    return {std::make_shared<PushUnionThroughJoin>(), std::make_shared<PushUnionThroughProjection>()};
}

std::vector<RulePtr> Rules::addRepartitionColumn()
{
    return {std::make_shared<AddRepartitionColumn>()};
}

std::vector<RulePtr> Rules::joinUsingToJoinOn()
{
    return {std::make_shared<JoinUsingToJoinOn>()};
}

std::vector<RulePtr> Rules::markTopNDistinct()
{
    return {std::make_shared<MarkTopNDistinctThroughExchange>()};
}

std::vector<RulePtr> Rules::pushTopNDistinct()
{
    return {std::make_shared<PushPartialTopNDistinctThroughExchange>()};
}

}
