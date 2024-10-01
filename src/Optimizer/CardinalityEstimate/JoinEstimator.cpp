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

#include <Optimizer/CardinalityEstimate/JoinEstimator.h>

#include <Core/Types.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/PredicateUtils.h>
#include <common/types.h>

namespace DB
{
PlanNodeStatisticsPtr JoinEstimator::estimate(
    PlanNodeStatisticsPtr & opt_left_stats,
    PlanNodeStatisticsPtr & opt_right_stats,
    const JoinStep & join_step,
    ContextMutablePtr & context,
    bool is_left_base_table,
    bool is_right_base_table,
    const std::vector<double> & children_filter_selectivity,
    const InclusionDependency & inclusion_dependency)
{
    if (!opt_left_stats || !opt_right_stats)
    {
        return nullptr;
    }

    PlanNodeStatistics & left_stats = *opt_left_stats;
    PlanNodeStatistics & right_stats = *opt_right_stats;

    const Names & left_keys = join_step.getLeftKeys();
    const Names & right_keys = join_step.getRightKeys();

    ASTTableJoin::Kind kind = join_step.getKind();
    PlanNodeStatisticsPtr res = computeCardinality(
        left_stats,
        right_stats,
        left_keys,
        right_keys,
        kind,
        join_step.getStrictness(),
        *context,
        is_left_base_table,
        is_right_base_table,
        children_filter_selectivity,
        inclusion_dependency);

    if (!res)
    {
        return nullptr;
    }

    // TODO@lichengxian update statistics for join filters.
    const auto & filter = join_step.getFilter();
    if ((kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Cross)
        && join_step.getStrictness() == ASTTableJoin::Strictness::All && filter && !PredicateUtils::isTruePredicate(filter))
    {
        double selectivity = context->getSettingsRef().stats_estimator_join_filter_selectivity;
        std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics_in_filter;
        // only cross join estimate directly inner join use default selectivity because of the symbol stats after cross join is accuracy.
        if (join_step.getLeftKeys().empty())
        {
            auto name_to_type = join_step.getInputStreams()[0].header.getNamesToTypes();
            auto name_to_type2 = join_step.getInputStreams()[1].header.getNamesToTypes();
            name_to_type.insert(name_to_type2.begin(), name_to_type2.end());
            auto interpreter = ExpressionInterpreter::basicInterpreter(name_to_type, context);
            FilterEstimatorContext estimator_context{
                .context = context,
                .interpreter = interpreter,
                .default_selectivity = context->getSettingsRef().stats_estimator_join_filter_selectivity};
            FilterEstimateResult result = FilterEstimator::estimateFilter(*res, filter, estimator_context);

            selectivity = result.first.value_or(estimator_context.default_selectivity);
            symbol_statistics_in_filter = result.second;

            if (selectivity <= 0.0)
            {
                selectivity = 0.01;
            }
            if (selectivity >= 1.0)
            {
                return res;
            }
        }

        auto before_filter_row_count = res->getRowCount();
        UInt64 filtered_row_count = std::round(res->getRowCount() * selectivity);
        // make row count at least 1.
        res->updateRowCount(filtered_row_count > 1 ? filtered_row_count : 1);
                for (auto & symbol_statistics : res->getSymbolStatistics())
        {
            // for symbol in filters. use the filtered statistics.
            if (symbol_statistics_in_filter.contains(symbol_statistics.first))
            {
                symbol_statistics.second = symbol_statistics_in_filter[symbol_statistics.first];
                symbol_statistics.second->getHistogram().clear();
            }
            else
            {
                symbol_statistics.second = symbol_statistics.second->applySelectivity(selectivity, symbol_statistics.second->getNdv() > before_filter_row_count * 0.8 ? selectivity : 1);
                // NDV must less or equals to row count
                symbol_statistics.second->setNdv(std::min(res->getRowCount(), symbol_statistics.second->getNdv()));
                symbol_statistics.second->getHistogram().clear();
            }
        }

        // make sure row count at least 1.
        return res;
    }

    return res;
}

PlanNodeStatisticsPtr JoinEstimator::computeCardinality(
    PlanNodeStatistics & left_stats,
    PlanNodeStatistics & right_stats,
    const Names & left_keys,
    const Names & right_keys,
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    Context & context,
    bool is_left_base_table,
    bool is_right_base_table,
    const std::vector<double> & children_filter_selectivity,
    const InclusionDependency & inclusion_dependency,
    bool only_cardinality)
{
    UInt64 left_rows = left_stats.getRowCount();
    UInt64 right_rows = right_stats.getRowCount();

    // init join card, and output column statistics.
    UInt64 join_card = left_rows * right_rows;
    std::unordered_map<String, SymbolStatisticsPtr> join_output_statistics;
    if (!only_cardinality)
        for (auto & item : left_stats.getSymbolStatistics())
        {
            join_output_statistics[item.first] = item.second->copy();
        }
    if (!only_cardinality)
        for (auto & item : right_stats.getSymbolStatistics())
        {
            join_output_statistics[item.first] = item.second->copy();
        }

    // cross join
    if (kind == ASTTableJoin::Kind::Cross)
    {
        for (const auto & item : left_stats.getSymbolStatistics())
        {
            join_output_statistics[item.first] = item.second->applySelectivity(right_rows, 1);
        }

        for (const auto & item : right_stats.getSymbolStatistics())
        {
            join_output_statistics[item.first] = item.second->applySelectivity(left_rows, 1);
        }

        return std::make_shared<PlanNodeStatistics>(join_card, std::move(join_output_statistics));
    }

    auto match_real_pk_fk = [&](const String & pk_name, const String & fk_name) -> bool {
        if (inclusion_dependency.empty())
            return false;

        auto pk_iter = inclusion_dependency.find(pk_name);
        auto fk_iter = inclusion_dependency.find(fk_name);
        if (pk_iter == inclusion_dependency.end() || fk_iter == inclusion_dependency.end())
            return false;
        if (fk_iter->second.first && !pk_iter->second.first) // check type of fk/pk.
        {
            if (fk_iter->second.second == pk_iter->second.second) // check ref_original_pk_name with original_pk_name.
            {
                return true;
            }
        }
        return false;
    };

    // inner/left/right/full join
    bool all_unknown_stat = true;
    for (size_t i = 0; i < left_keys.size(); ++i)
    {
        const String & left_key = left_keys.at(i);
        const String & right_key = right_keys.at(i);

        SymbolStatistics & left_key_stats = *left_stats.getSymbolStatistics(left_key);
        SymbolStatistics & right_key_stats = *right_stats.getSymbolStatistics(right_key);

        if (left_key_stats.isUnknown() || right_key_stats.isUnknown())
        {
            continue;
        }
        all_unknown_stat = false;

        UInt64 left_ndv = left_key_stats.getNdv();
        UInt64 right_ndv = right_key_stats.getNdv();

        String left_db_table_column = left_key_stats.getDbTableColumn();
        String right_db_table_column = right_key_stats.getDbTableColumn();

        // join output cardinality and join output column statistics for every join key
        UInt64 pre_key_join_card;
        std::unordered_map<String, SymbolStatisticsPtr> pre_key_join_output_statistics;
        bool enable_pk_fk = context.getSettingsRef().enable_pk_fk;
        bool enable_real_pk_fk = context.getSettingsRef().enable_real_pk_fk;

        // case 1 : left join key equals to right join key. (self-join)
        if (left_db_table_column == right_db_table_column)
        {
            pre_key_join_card = computeCardinalityByNDV(
                left_stats,
                right_stats,
                left_key_stats,
                right_key_stats,
                kind,
                strictness,
                left_key,
                right_key,
                pre_key_join_output_statistics,
                only_cardinality);
        }

        // case 2 : PK join FK
        else if (
            (enable_pk_fk && matchPKFK(left_rows, right_rows, left_ndv, right_ndv))
            || (enable_real_pk_fk && match_real_pk_fk(left_key, right_key)))
        {
            pre_key_join_card = computeCardinalityByFKPK(
                right_rows,
                right_ndv,
                left_ndv,
                context.getSettingsRef().pk_selectivity,
                right_stats,
                left_stats,
                right_key_stats,
                left_key_stats,
                right_key,
                left_key,
                is_right_base_table,
                is_left_base_table,
                context.getSettingsRef().enable_filtered_pk_selectivity && children_filter_selectivity.size() == 2
                    ? children_filter_selectivity[0]
                    : 1.0,
                pre_key_join_output_statistics,
                only_cardinality);
        }

        // case 3 : FK join PK
        else if (
            (enable_pk_fk && matchFKPK(left_rows, right_rows, left_ndv, right_ndv))
            || (enable_real_pk_fk && match_real_pk_fk(right_key, left_key)))
        {
            pre_key_join_card = computeCardinalityByFKPK(
                left_rows,
                left_ndv,
                right_ndv,
                context.getSettingsRef().pk_selectivity,
                left_stats,
                right_stats,
                left_key_stats,
                right_key_stats,
                left_key,
                right_key,
                is_left_base_table,
                is_right_base_table,
                context.getSettingsRef().enable_filtered_pk_selectivity && children_filter_selectivity.size() == 2
                    ? children_filter_selectivity[1]
                    : 1.0,
                pre_key_join_output_statistics,
                only_cardinality);
        }

        // case 4 : normal join cases, with histogram exist.
        else if (context.getSettingsRef().stats_estimator_join_use_histogram && !left_key_stats.getHistogram().getBuckets().empty() && !right_key_stats.getHistogram().getBuckets().empty())
        {
            pre_key_join_card = computeCardinalityByHistogram(
                left_stats,
                right_stats,
                left_key_stats,
                right_key_stats,
                kind,
                strictness,
                left_key,
                right_key,
                pre_key_join_output_statistics,
                only_cardinality);
        }
        else
        {
            // case 4 : normal join cases, with histogram not exist.
            pre_key_join_card = computeCardinalityByNDV(
                left_stats,
                right_stats,
                left_key_stats,
                right_key_stats,
                kind,
                strictness,
                left_key,
                right_key,
                pre_key_join_output_statistics,
                only_cardinality);
        }

        // we choose the smallest one.
        if (pre_key_join_card <= join_card)
        {
            join_card = pre_key_join_card;
            join_output_statistics.swap(pre_key_join_output_statistics);
        }
    }

    // not cross join and can't estimate
    if (all_unknown_stat && !left_keys.empty())
    {
        if (context.getSettingsRef().enable_estimate_without_symbol_statistics)
            return std::make_shared<PlanNodeStatistics>(std::max(left_rows, right_rows), std::unordered_map<String, SymbolStatisticsPtr>{});
        return nullptr;
    }

    // Adjust the number of output rows by join kind.

    // Consider correlated with multi join keys.
    if (kind == ASTTableJoin::Kind::Inner && left_keys.size() > 1)
    {
        double adjust_correlated_coefficient
            = std::pow(context.getSettingsRef().multi_join_keys_correlated_coefficient, left_keys.size() - 1);
        join_card *= adjust_correlated_coefficient;
    }

    // All rows from left side should be in the result.
    if (kind == ASTTableJoin::Kind::Left)
    {
        if (strictness == ASTTableJoin::Strictness::Anti)
        {
            join_card = left_rows * context.getSettingsRef().stats_estimator_anti_join_filter_coefficient;
            if (left_rows > join_card)
                join_card = std::max(join_card, left_rows - join_card);
        }
        else if (strictness == ASTTableJoin::Strictness::Semi)
        {
            join_card = std::min(left_rows, join_card);
        }
        else
        {
            join_card = std::max(left_rows, join_card);
        }
    }
    // All rows from right side should be in the result.
    if (kind == ASTTableJoin::Kind::Right)
    {
        if (strictness == ASTTableJoin::Strictness::Anti)
        {
            join_card = right_rows * context.getSettingsRef().stats_estimator_anti_join_filter_coefficient;
            if (right_rows > join_card)
                join_card = std::max(join_card, right_rows - join_card);
        }
        else if (strictness == ASTTableJoin::Strictness::Semi)
        {
            join_card = std::min(right_rows, join_card);
        }
        else
        {
            join_card = std::max(right_rows, join_card);
        }
    }
    // T(A FOJ B) = T(A LOJ B) + T(A ROJ B) - T(A IJ B)
    if (kind == ASTTableJoin::Kind::Full)
    {
        join_card = std::max(left_rows, join_card) + std::max(right_rows, join_card) - join_card;
    }

    // normalize output column NDV
    for (auto & output_statistics : join_output_statistics)
    {
        if (output_statistics.second->getNdv() > join_card)
        {
            output_statistics.second->setNdv(join_card);
        }
    }

    return std::make_shared<PlanNodeStatistics>(join_card, join_output_statistics);
}

bool JoinEstimator::matchPKFK(UInt64 left_rows, UInt64 right_rows, UInt64 left_ndv, UInt64 right_ndv)
{
    if (left_ndv == 0 || right_ndv == 0)
    {
        return false;
    }

    // PK-FK assumption
    bool is_left_pk = left_rows == left_ndv;

    // as NDV is calculate by hyperloglog algorithm, it is not exactly.
    bool is_left_almost_pk = false;
    if (!is_left_pk)
    {
        if (left_rows > left_ndv)
        {
            is_left_almost_pk = (double(left_ndv) / left_rows) > 0.95;
        }
        else
        {
            is_left_almost_pk = (double(left_rows) / left_ndv) > 0.95;
        }
    }
    bool is_right_fk = right_rows > right_ndv;

    return (is_left_pk || is_left_almost_pk) && is_right_fk;
}

bool JoinEstimator::matchFKPK(UInt64 left_rows, UInt64 right_rows, UInt64 left_ndv, UInt64 right_ndv)
{
    if (left_ndv == 0 || right_ndv == 0)
    {
        return false;
    }

    // FK-PK assumption
    bool is_left_fk = left_rows > left_ndv;
    bool is_right_pk = right_rows == right_ndv;

    // as NDV is calculate by hyperloglog algorithm, it is not exactly.
    bool is_right_almost_pk = false;
    if (!is_right_pk)
    {
        if (right_rows > right_ndv)
        {
            is_right_almost_pk = (static_cast<double>(right_ndv) / right_rows) > 0.95;
        }
        else
        {
            is_right_almost_pk = (static_cast<double>(right_rows) / right_ndv) > 0.95;
        }
    }

    return is_left_fk && (is_right_pk || is_right_almost_pk);
}

UInt64 JoinEstimator::computeCardinalityByFKPK(
    UInt64 fk_rows,
    UInt64 fk_ndv,
    UInt64 pk_ndv,
    [[maybe_unused]] double pk_selectivity,
    PlanNodeStatistics & fk_stats,
    PlanNodeStatistics & pk_stats,
    SymbolStatistics & fk_key_stats,
    SymbolStatistics & pk_key_stats,
    String fk_key,
    String pk_key,
    [[maybe_unused]] bool is_fk_base_table,
    [[maybe_unused]] bool is_pk_base_table,
    double pk_filter_selectivity,
    std::unordered_map<String, SymbolStatisticsPtr> & join_output_statistics,
    bool only_cardinality)
{
    // if FK side ndv less then FK side ndv.
    // it means all FKs can be joined, but only part of PK can be joined.

    double join_card = static_cast<double>(fk_rows * pk_ndv) / fk_ndv;

    if (join_card > fk_rows)
    {
        join_card = fk_rows;
    }

    // FK side all match;
    if (fk_ndv <= pk_ndv)
    {
        join_card = fk_rows;
        if (pk_filter_selectivity < 0.99)
        {
            join_card *= pk_filter_selectivity;
        }
        if (!only_cardinality)
            for (auto & item : fk_stats.getSymbolStatistics())
            {
                join_output_statistics[item.first] = item.second->copy();
            }

        // PK side partial match, adjust statistics;
        double adjust_rowcount = join_card / pk_stats.getRowCount();
        double adjust_ndv = static_cast<double>(fk_ndv) / pk_ndv;
        if (!only_cardinality)
            for (auto & item : pk_stats.getSymbolStatistics())
            {
                if (item.first == pk_key)
                {
                    auto new_pk_key_stats = pk_key_stats.applySelectivity(adjust_rowcount, adjust_ndv);
                    new_pk_key_stats->setNdv(fk_ndv);
                    join_output_statistics[item.first] = new_pk_key_stats;
                }
                else
                {
                    if (static_cast<double>(item.second->getNdv()) / pk_stats.getRowCount() > 0.8)
                    {
                        join_output_statistics[item.first] = item.second->applySelectivity(adjust_rowcount, adjust_ndv);
                    }
                    else
                    {
                        join_output_statistics[item.first] = item.second->applySelectivity(adjust_rowcount, 1);
                    }
                }
            }
    }

    // if FK side ndv large then PK side ndv.
    // it means all PKs can be joined, but only part of PK can be joined.
    if (fk_ndv > pk_ndv)
    {
        join_card = static_cast<double>(fk_rows * pk_ndv) / fk_ndv;

        double adjust_fk_rowcount = join_card / fk_stats.getRowCount();
        double adjust_fk_ndv = static_cast<double>(pk_ndv) / fk_ndv;
        if (!only_cardinality)
            for (auto & item : fk_stats.getSymbolStatistics())
            {
                if (item.first == fk_key)
                {
                    auto new_fk_key_stats = fk_key_stats.applySelectivity(adjust_fk_rowcount, adjust_fk_ndv);
                    new_fk_key_stats->setNdv(pk_ndv);
                    join_output_statistics[item.first] = new_fk_key_stats;
                }
                else
                {
                    if (static_cast<double>(item.second->getNdv()) / fk_stats.getRowCount() > 0.8)
                    {
                        join_output_statistics[item.first] = item.second->applySelectivity(adjust_fk_rowcount, adjust_fk_ndv);
                    }
                    else
                    {
                        join_output_statistics[item.first] = item.second->applySelectivity(adjust_fk_rowcount, 1);
                    }
                }
            }

        double adjust_pk_rowcount = join_card / pk_stats.getRowCount();
        if (!only_cardinality)
            for (auto & item : pk_stats.getSymbolStatistics())
            {
                join_output_statistics[item.first] = item.second->applySelectivity(adjust_pk_rowcount, 1.0);
            }
    }


    return join_card;
}

static bool isInnerOrLeftSemi(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness)
{
    return kind == ASTTableJoin::Kind::Inner
        || (kind == ASTTableJoin::Kind::Left
            && (strictness == ASTTableJoin::Strictness::Semi || strictness == ASTTableJoin::Strictness::Any));
}

static bool isInnerOrRightSemi(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness)
{
    return kind == ASTTableJoin::Kind::Inner
        || (kind == ASTTableJoin::Kind::Right
            && (strictness == ASTTableJoin::Strictness::Semi || strictness == ASTTableJoin::Strictness::Any));
}

UInt64 JoinEstimator::computeCardinalityByHistogram(
    PlanNodeStatistics & left_stats,
    PlanNodeStatistics & right_stats,
    SymbolStatistics & left_key_stats,
    SymbolStatistics & right_key_stats,
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    String left_key,
    String right_key,
    std::unordered_map<String, SymbolStatisticsPtr> & join_output_statistics,
    bool only_cardinality)
{
    // Choose the maximum of two min values, and the minimum of two max values.
    double min = std::max(left_key_stats.getMin(), right_key_stats.getMin());
    double max = std::min(left_key_stats.getMax(), right_key_stats.getMax());

    auto left_key_ndv = left_key_stats.getNdv();
    auto right_key_ndv = right_key_stats.getNdv();
    UInt64 min_ndv = std::min(left_key_ndv, right_key_ndv);

    const Histogram & left_his = left_key_stats.getHistogram();
    const Histogram & right_his = right_key_stats.getHistogram();

    Buckets join_buckets = left_his.estimateJoin(right_his, min, max);

    double join_card = 1;
    for (auto & bucket : join_buckets)
    {
        join_card += bucket.getCount();
    }

    double adjust_left_rowcount = join_card / left_stats.getRowCount();
    if (!only_cardinality)
        for (auto & item : left_stats.getSymbolStatistics())
        {
            if (item.first == left_key)
            {
                auto new_left_key_stats = left_key_stats.createJoin(join_buckets);
                if (isInnerOrLeftSemi(kind, strictness))
                {
                    new_left_key_stats->setNdv(min_ndv);
                }
                join_output_statistics[left_key] = new_left_key_stats;
            }
            else
            {
                join_output_statistics[item.first] = item.second->applySelectivity(adjust_left_rowcount, 1);
            }
        }

    double adjust_right_rowcount = join_card / right_stats.getRowCount();
    if (!only_cardinality)
        for (auto & item : right_stats.getSymbolStatistics())
        {
            if (item.first == right_key)
            {
                auto new_right_key_stats = right_key_stats.createJoin(join_buckets);
                if (isInnerOrRightSemi(kind, strictness))
                {
                    new_right_key_stats->setNdv(min_ndv);
                }
                join_output_statistics[right_key] = new_right_key_stats;
            }
            else
            {
                join_output_statistics[item.first] = item.second->applySelectivity(adjust_right_rowcount, 1);
            }
        }

    return join_card;
}

UInt64 JoinEstimator::computeCardinalityByNDV(
    PlanNodeStatistics & left_stats,
    PlanNodeStatistics & right_stats,
    SymbolStatistics & left_key_stats,
    SymbolStatistics & right_key_stats,
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    String left_key,
    String right_key,
    std::unordered_map<String, SymbolStatisticsPtr> & join_output_statistics,
    bool only_cardinality)
{
    UInt64 multiply_card = left_stats.getRowCount() * right_stats.getRowCount();
    UInt64 max_ndv = std::max(left_key_stats.getNdv(), right_key_stats.getNdv());
    UInt64 min_ndv = std::min(left_key_stats.getNdv(), right_key_stats.getNdv());

    double join_card = max_ndv == 0 ? 1 : multiply_card / max_ndv;

    double adjust_left_rowcount = join_card / left_stats.getRowCount();
    if (!only_cardinality)
        for (auto & item : left_stats.getSymbolStatistics())
        {
            join_output_statistics[item.first] = item.second->applySelectivity(adjust_left_rowcount, 1.0);
            if (item.first == left_key && isInnerOrLeftSemi(kind, strictness))
            {
                join_output_statistics[item.first]->setNdv(min_ndv);
            }
        }
    double adjust_right_rowcount = join_card / right_stats.getRowCount();
    if (!only_cardinality)
        for (auto & item : right_stats.getSymbolStatistics())
        {
            join_output_statistics[item.first] = item.second->applySelectivity(adjust_right_rowcount, 1.0);
            if (item.first == right_key && isInnerOrRightSemi(kind, strictness))
            {
                join_output_statistics[item.first]->setNdv(min_ndv);
            }
        }
    return join_card;
}

}
