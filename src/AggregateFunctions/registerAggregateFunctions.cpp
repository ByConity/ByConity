/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <AggregateFunctions/registerAggregateFunctions.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{
struct Settings;

class AggregateFunctionFactory;
void registerAggregateFunctionArbitrary(AggregateFunctionFactory &);
void registerAggregateFunctionAvg(AggregateFunctionFactory &);
void registerAggregateFunctionAvgWeighted(AggregateFunctionFactory &);
void registerAggregateFunctionCount(AggregateFunctionFactory &);
void registerAggregateFunctionDeltaSum(AggregateFunctionFactory &);
void registerAggregateFunctionDeltaSumTimestamp(AggregateFunctionFactory &);
void registerAggregateFunctionGroupArray(AggregateFunctionFactory &);
void registerAggregateFunctionGroupConcat(AggregateFunctionFactory &);
void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory &);
void registerAggregateFunctionGroupArrayInsertAt(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantile(AggregateFunctionFactory &);
void registerAggregateFunctionQuantileTDigestWithSSize(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileInterpolatedWeighted(AggregateFunctionFactory &);
void registerAggregateFunctionsQuantileBFloat16Weighted(AggregateFunctionFactory &);
void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory &);
void registerAggregateFunctionWindowFunnel(AggregateFunctionFactory &);
void registerAggregateFunctionRate(AggregateFunctionFactory &);
void registerAggregateFunctionsMin(AggregateFunctionFactory &);
void registerAggregateFunctionsMax(AggregateFunctionFactory &);
void registerAggregateFunctionsAny(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsStable(AggregateFunctionFactory &);
void registerAggregateFunctionsStatisticsSimple(AggregateFunctionFactory &);
void registerAggregateFunctionsVarianceMatrix(AggregateFunctionFactory &);
void registerAggregateFunctionSum(AggregateFunctionFactory &);
void registerAggregateFunctionSumCount(AggregateFunctionFactory &);
void registerAggregateFunctionSumMap(AggregateFunctionFactory &);
void registerAggregateFunctionsUniq(AggregateFunctionFactory &);
void registerAggregateFunctionUniqCombined(AggregateFunctionFactory &);
void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory &);
void registerAggregateFunctionTopK(AggregateFunctionFactory &);
void registerAggregateFunctionsBitwise(AggregateFunctionFactory &);
void registerAggregateFunctionsBitmap(AggregateFunctionFactory &);
void registerAggregateFunctionsMaxIntersections(AggregateFunctionFactory &);
void registerAggregateFunctionHistogram(AggregateFunctionFactory &);
void registerAggregateFunctionRetention(AggregateFunctionFactory &);
void registerAggregateFunctionMLMethod(AggregateFunctionFactory &);
void registerAggregateFunctionEntropy(AggregateFunctionFactory &);
void registerAggregateFunctionSimpleLinearRegression(AggregateFunctionFactory &);
void registerAggregateFunctionMoving(AggregateFunctionFactory &);
void registerAggregateFunctionCategoricalIV(AggregateFunctionFactory &);
void registerAggregateFunctionAggThrow(AggregateFunctionFactory &);
void registerAggregateFunctionRankCorrelation(AggregateFunctionFactory &);
void registerAggregateFunctionMannWhitney(AggregateFunctionFactory &);
void registerAggregateFunctionWelchTTest(AggregateFunctionFactory &);
void registerAggregateFunctionStudentTTest(AggregateFunctionFactory &);
void registerAggregateFunctionMeanZTest(AggregateFunctionFactory &);
void registerAggregateFunctionCramersV(AggregateFunctionFactory &);
void registerAggregateFunctionTheilsU(AggregateFunctionFactory &);
void registerAggregateFunctionContingency(AggregateFunctionFactory &);
void registerAggregateFunctionCramersVBiasCorrected(AggregateFunctionFactory &);
void registerAggregateFunctionSingleValueOrNull(AggregateFunctionFactory &);
void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory &);
void registerAggregateFunctionExponentialMovingAverage(AggregateFunctionFactory &);
void registerAggregateFunctionSparkbar(AggregateFunctionFactory &);
void registerAggregateFunctionMergeStreamStack(AggregateFunctionFactory &);
void registerAggregateFunctionSessionSplit(AggregateFunctionFactory &);
void registerAggregateFunctionSessionAnalysis(AggregateFunctionFactory & factory);
void registerAggregateFunctionRetention4(AggregateFunctionFactory &);
void registerAggregateFunctionRetention2(AggregateFunctionFactory &);
void registerAggregateFunctionRetentionLoss(AggregateFunctionFactory &);
void registerAggregateFunctionGenArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionGenArrayMonth(AggregateFunctionFactory & factory);
void registerAggregateFunctionAttribution(AggregateFunctionFactory &);
void registerAggregateFunctionAttributionAnalysis(AggregateFunctionFactory &);
void registerAggregateFunctionAttributionAnalysisFuse(AggregateFunctionFactory &);
void registerAggregateFunctionAttributionCorrelation(AggregateFunctionFactory &);
void registerAggregateFunctionAttributionCorrelationFuse(AggregateFunctionFactory &);
void registerAggregateFunctionFinderFunnel(AggregateFunctionFactory & factory);
void registerAggregateFunctionFinderGroupFunnel(AggregateFunctionFactory & factory);
void registerAggregateFunctionFunnelRep(AggregateFunctionFactory & factory);
void registerAggregateFunctionFunnelRep2(AggregateFunctionFactory & factory);
void registerAggregateFunctionFunnelRep3(AggregateFunctionFactory & factory);
void registerAggregateFunctionUserDistribution(AggregateFunctionFactory & factory);
void registerAggregateFunctionUserDistributionMonthly(AggregateFunctionFactory & factory);
void registerAggregateFunctionLastRangeCount(AggregateFunctionFactory & factory);
void registerAggregateFunctionSlideMatchCount(AggregateFunctionFactory & factory);
void registerAggregateFunctionFrequency(AggregateFunctionFactory & factory);
void registerAggregateFunctionPathSplit(AggregateFunctionFactory & factory);
void registerAggregateFunctionPathCount(AggregateFunctionFactory & factory);
void registerAggregateFunctionFunnelPathSplit(AggregateFunctionFactory & factory);
void registerAggregateFunctionXirr(AggregateFunctionFactory & factory);
void registerAggregateFunctionsBitmapLogic(AggregateFunctionFactory & factory);
void registerAggregateFunctionsBitmapFromColumn(AggregateFunctionFactory &);
void registerAggregateFunctionsBitmapColumnDiff(AggregateFunctionFactory & factory);
void registerAggregateFunctionsBitmapExpressionCalculation(AggregateFunctionFactory & factory);
void registerAggregateFunctionsBitmapMaxLevel(AggregateFunctionFactory & factory);
void registerAggregateFunctionsBitMapJoin(AggregateFunctionFactory & factory);
void registerAggregateFunctionsBitMapJoinAndCard(AggregateFunctionFactory & factory);
void registerAggregateFunctionHllSketch(AggregateFunctionFactory & factory);
void registerAggregateFunctionKllSketch(AggregateFunctionFactory & factory);
void registerAggregateFunctionNdvBuckets(AggregateFunctionFactory & factory);
void registerAggregateFunctionNdvBucketsExtend(AggregateFunctionFactory & factory);
void registerAggregateFunctionNothing(AggregateFunctionFactory & factory);
void registerAggregateFunctionHllSketchEstimate(AggregateFunctionFactory &);
void registerAggregateFunctionAuc(AggregateFunctionFactory &);
void registerAggregateFunctionFastAuc(AggregateFunctionFactory &);
void registerAggregateFunctionFastAuc2(AggregateFunctionFactory &);
void registerAggregateFunctionFastAuc3(AggregateFunctionFactory &);
void registerAggregateFunctionRegAuc(AggregateFunctionFactory &);
void registerAggregateFunctionRegAucV2(AggregateFunctionFactory &);
void registerAggregateFunctionDebiasAuc(AggregateFunctionFactory &);
void registerAggregateFunctionEcpmAuc(AggregateFunctionFactory &);
void registerAggregateFunctionNdcg(AggregateFunctionFactory &);

class AggregateFunctionCombinatorFactory;
void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorArray(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorForEach(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorSimpleState(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorState(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorMerge(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorOrFill(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorResample(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorDistinct(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorStack(AggregateFunctionCombinatorFactory &);
void registerAggregateFunctionCombinatorMap(AggregateFunctionCombinatorFactory & factory);

void registerWindowFunctions(AggregateFunctionFactory & factory);

void registerAggregateFunctionIntervalLengthSum(AggregateFunctionFactory &);

void registerAggregateFunctions()
{
    {
        auto & factory = AggregateFunctionFactory::instance();

        registerAggregateFunctionArbitrary(factory);
        registerAggregateFunctionAvg(factory);
        registerAggregateFunctionAvgWeighted(factory);
        registerAggregateFunctionCount(factory);
        registerAggregateFunctionDeltaSum(factory);
        registerAggregateFunctionDeltaSumTimestamp(factory);
        registerAggregateFunctionGroupArray(factory);
        registerAggregateFunctionGroupConcat(factory);
        registerAggregateFunctionGroupUniqArray(factory);
        registerAggregateFunctionGroupArrayInsertAt(factory);
        registerAggregateFunctionsQuantile(factory);
        registerAggregateFunctionQuantileTDigestWithSSize(factory);
        registerAggregateFunctionsQuantileInterpolatedWeighted(factory);
        registerAggregateFunctionsQuantileBFloat16Weighted(factory);
        registerAggregateFunctionsSequenceMatch(factory);
        registerAggregateFunctionWindowFunnel(factory);
        registerAggregateFunctionRate(factory);
        registerAggregateFunctionsMin(factory);
        registerAggregateFunctionsMax(factory);
        registerAggregateFunctionsAny(factory);
        registerAggregateFunctionsStatisticsStable(factory);
        registerAggregateFunctionsStatisticsSimple(factory);
        registerAggregateFunctionsVarianceMatrix(factory);
        registerAggregateFunctionSum(factory);
        registerAggregateFunctionSumCount(factory);
        registerAggregateFunctionSumMap(factory);
        registerAggregateFunctionsUniq(factory);
        registerAggregateFunctionUniqCombined(factory);
        registerAggregateFunctionUniqUpTo(factory);
        registerAggregateFunctionTopK(factory);
        registerAggregateFunctionsBitwise(factory);
        registerAggregateFunctionCramersV(factory);
        registerAggregateFunctionTheilsU(factory);
        registerAggregateFunctionContingency(factory);
        registerAggregateFunctionCramersVBiasCorrected(factory);
        registerAggregateFunctionSingleValueOrNull(factory);
#if !defined(ARCADIA_BUILD)
        registerAggregateFunctionsBitmap(factory);
#endif
        registerAggregateFunctionsMaxIntersections(factory);
        registerAggregateFunctionHistogram(factory);
        registerAggregateFunctionRetention(factory);
        registerAggregateFunctionMLMethod(factory);
        registerAggregateFunctionEntropy(factory);
        registerAggregateFunctionSimpleLinearRegression(factory);
        registerAggregateFunctionMoving(factory);
        registerAggregateFunctionCategoricalIV(factory);
        registerAggregateFunctionAggThrow(factory);
        registerAggregateFunctionRankCorrelation(factory);
        registerAggregateFunctionMannWhitney(factory);
        registerAggregateFunctionSequenceNextNode(factory);
        registerAggregateFunctionWelchTTest(factory);
        registerAggregateFunctionStudentTTest(factory);
        registerAggregateFunctionMeanZTest(factory);
        registerAggregateFunctionExponentialMovingAverage(factory);
        registerAggregateFunctionSparkbar(factory);
        registerAggregateFunctionMergeStreamStack(factory);
        registerAggregateFunctionSessionSplit(factory);
        registerAggregateFunctionSessionAnalysis(factory);
        registerAggregateFunctionRetention4(factory);
        registerAggregateFunctionRetention2(factory);
        registerAggregateFunctionRetentionLoss(factory);
        registerAggregateFunctionGenArray(factory);
        registerAggregateFunctionGenArrayMonth(factory);
        registerAggregateFunctionAttribution(factory);
        registerAggregateFunctionAttributionAnalysis(factory);
        registerAggregateFunctionAttributionAnalysisFuse(factory);
        registerAggregateFunctionAttributionCorrelation(factory);
        registerAggregateFunctionAttributionCorrelationFuse(factory);
        registerAggregateFunctionFinderFunnel(factory);
        registerAggregateFunctionFinderGroupFunnel(factory);
        registerAggregateFunctionFunnelRep(factory);
        registerAggregateFunctionFunnelRep2(factory);
        registerAggregateFunctionFunnelRep3(factory);
        registerAggregateFunctionUserDistribution(factory);
        registerAggregateFunctionUserDistributionMonthly(factory);
        registerAggregateFunctionLastRangeCount(factory);
        registerAggregateFunctionSlideMatchCount(factory);
        registerAggregateFunctionFrequency(factory);
        registerAggregateFunctionPathSplit(factory);
        registerAggregateFunctionPathCount(factory);
        registerAggregateFunctionFunnelPathSplit(factory);
        registerAggregateFunctionXirr(factory);

        registerWindowFunctions(factory);

        registerAggregateFunctionIntervalLengthSum(factory);
        registerAggregateFunctionsBitmapFromColumn(factory);
        registerAggregateFunctionsBitmapLogic(factory);
        registerAggregateFunctionsBitmapColumnDiff(factory);
        registerAggregateFunctionsBitmapExpressionCalculation(factory);
        registerAggregateFunctionsBitmapMaxLevel(factory);
        registerAggregateFunctionsBitMapJoin(factory);
        registerAggregateFunctionsBitMapJoinAndCard(factory);
        registerAggregateFunctionHllSketch(factory);
        registerAggregateFunctionKllSketch(factory);
        registerAggregateFunctionNdvBuckets(factory);
        registerAggregateFunctionNdvBucketsExtend(factory);
        registerAggregateFunctionNothing(factory);
        registerAggregateFunctionHllSketchEstimate(factory);
        registerAggregateFunctionAuc(factory);
        registerAggregateFunctionFastAuc(factory);
        registerAggregateFunctionFastAuc2(factory);
        registerAggregateFunctionFastAuc3(factory);
        registerAggregateFunctionRegAuc(factory);
        registerAggregateFunctionRegAucV2(factory);
        registerAggregateFunctionDebiasAuc(factory);
        registerAggregateFunctionEcpmAuc(factory);
        registerAggregateFunctionNdcg(factory);
    }

    {
        auto & factory = AggregateFunctionCombinatorFactory::instance();

        registerAggregateFunctionCombinatorIf(factory);
        registerAggregateFunctionCombinatorArray(factory);
        registerAggregateFunctionCombinatorForEach(factory);
        registerAggregateFunctionCombinatorSimpleState(factory);
        registerAggregateFunctionCombinatorState(factory);
        registerAggregateFunctionCombinatorMerge(factory);
        registerAggregateFunctionCombinatorNull(factory);
        registerAggregateFunctionCombinatorOrFill(factory);
        registerAggregateFunctionCombinatorResample(factory);
        registerAggregateFunctionCombinatorDistinct(factory);
        registerAggregateFunctionCombinatorStack(factory);
        registerAggregateFunctionCombinatorMap(factory);
    }
}

}
