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

#pragma once

#include <Storages/MergeTree/MergeSelector.h>


/**
We have a set of data parts that is dynamically changing - new data parts are added and there is background merging process.
Background merging process periodically selects continuous range of data parts to merge.

It tries to optimize the following metrics:
1. Write amplification: total amount of data written on disk (source data + merges) relative to the amount of source data.
It can be also considered as the total amount of work for merges.
2. The number of data parts in the set at the random moment of time (average + quantiles).

Also taking the following considerations:
1. Older data parts should be merged less frequently than newer data parts.
2. Larger data parts should be merged less frequently than smaller data parts.
3. If no new parts arrive, we should continue to merge existing data parts to eventually optimize the table.
4. Never allow too many parts, because it will slow down SELECT queries significantly.
5. Multiple background merges can run concurrently but not too many.

It is not possible to optimize both metrics, because they contradict to each other.
To lower the number of parts we can merge eagerly but write amplification will increase.
Then we need some balance between optimization of these two metrics.

But some optimizations may improve both metrics.

For example, we can look at the "merge tree" - the tree of data parts that were merged.
If the tree is perfectly balanced then its depth is proportonal to the log(data size),
the total amount of work is proportional to data_size * log(data_size)
and the write amplification is proportional to log(data_size).
If it's not balanced (e.g. every new data part is always merged with existing data parts),
its depth is proportional to the data size, total amount of work is proportional to data_size^2.

We can also control the "base of the logarithm" - you can look it as the number of data parts
that are merged at once (the tree "arity"). But as the data parts are of different size, we should generalize it:
calculate the ratio between total size of merged parts to the size of the largest part participated in merge.
For example, if we merge 4 parts of size 5, 3, 1, 1 - then "base" will be 2 (10 / 5).

Base of the logarithm (simply called `base` in `SimpleMergeSelector`) is the main knob to control the write amplification.
The more it is, the less is write amplification but we will have more data parts on average.

To fit all the considerations, we also adjust `base` depending on total parts count,
parts size and parts age, with linear interpolation (then `base` is not a constant but a function of multiple variables,
looking like a section of hyperplane).


Then we apply the algorithm to select the optimal range of data parts to merge.
There is a proof that this algorithm is optimal if we look in the future only by single step.

The best range of data parts is selected.

We also apply some tunes:
- there is a fixed const of merging small parts (that is added to the size of data part before all estimations);
- there are some heuristics to "stick" ranges to large data parts.

It's still unclear if this algorithm is good or optimal at all. It's unclear if this algorithm is using the optimal coefficients.

To test and optimize SimpleMergeSelector, we apply the following methods:
- insert/merge simulator: a model simulating parts insertion and merging;
merge selecting algorithm is applied and the relevant metrics are calculated to allow to tune the algorithm;
- insert/merge simulator on real `system.part_log` from production - it gives realistic information about inserted data parts:
their sizes, at what time intervals they are inserted;

There is a research thesis dedicated to optimization of merge algorithm:
https://presentations.clickhouse.tech/hse_2019/merge_algorithm.pptx

This work made attempt to variate the coefficients in SimpleMergeSelector and to solve the optimization task:
maybe some change in coefficients will give a clear win on all metrics. Surprisingly enough, it has found
that our selection of coefficients is near optimal. It has found slightly more optimal coefficients,
but I decided not to use them, because the representativeness of the test data is in question.

This work did not make any attempt to propose any other algorithm.
This work did not make any attempt to analyze the task with analytical methods.
That's why I still believe that there are many opportunities to optimize the merge selection algorithm.

Please do not mix the task with a similar task in other LSM-based systems (like RocksDB).
Their problem statement is subtly different. Our set of data parts is consisted of data parts
that are completely independent in stored data. Ranges of primary keys in data parts can intersect.
When doing SELECT we read from all data parts. INSERTed data parts comes with unknown size...
*/

namespace DB
{

class MergeScheduler;

#define LIST_OF_SIMPLE_MERGE_SELECTOR_SETTINGS(M) \
    /** Zero means unlimited. Can be overridden by the same merge tree setting. */ \
    M(UInt64, max_parts_to_merge_at_once, 100, "", 0) \
    M(UInt64, min_parts_to_merge_at_once, 3, "", 0) \
\
    /** Zero means unlimited.*/ \
    /** Unique table will set it to a value < 2^32 in order to prevent rowid(UInt32) overflow */ \
    /** Too large part has no advantage since we cannot utilize parallelism. We set max_total_rows_to_merge as 2147483647.*/ \
    M(UInt64, max_total_rows_to_merge, 0xFFFFFFFF, "", 0) \
\
    /** Minimum ratio of size of one part to all parts in set of parts to merge (for usual cases).*/ \
    /** For example, if all parts have equal size, it means, that at least 'base' number of parts should be merged.*/ \
    /** If parts has non-uniform sizes, then minimum number of parts to merge is effectively increased.*/ \
    /** This behaviour balances merge-tree workload.*/ \
    /** It called 'base', because merge-tree depth could be estimated as logarithm with that base.*/ \
    /** If base is higher - then tree gets more wide and narrow, lowering write amplification.*/ \
    /** If base is lower - then merges occurs more frequently, lowering number of parts in average.*/ \
    /** */ \
    /** We need some balance between write amplification and number of parts.*/ \
    M(Float, base, 5, "", 0) \
    /** Simple standard baseline for merge to lower the base. */ \
    M(Float, standard_baseline, 2.0, "", 0) \
\
    /** Base is lowered until 1 (effectively means "merge any two parts") depending on several variables:*/ \
    /** */ \
    /** 1. Total number of parts in partition. If too many - then base is lowered.*/ \
    /** It means: when too many parts - do merges more urgently.*/ \
    /** */ \
    /** 2. Minimum age of parts participating in merge. If higher age - then base is lowered.*/ \
    /** It means: do less wide merges only rarely.*/ \
    /** */ \
    /** 3. Sum size of parts participating in merge. If higher - then more age is required to lower base. So, base is lowered slower.*/ \
    /** It means: for small parts, it's worth to merge faster, even not so wide or balanced.*/ \
    /** */ \
    /** We have multivariative dependency. Let it be logarithmic of size and somewhat multi-linear by other variables,*/ \
    /** between some boundary points, and constant outside.*/ \
    M(UInt64, min_size_to_lower_base, 1024 * 1024, "", 0) \
    M(UInt64, max_size_to_lower_base, 100ULL * 1024 * 1024 * 1024, "", 0) \
\
    M(UInt64, min_age_to_lower_base_at_min_size, 10, "", 0) \
    M(UInt64, min_age_to_lower_base_at_max_size, 10, "", 0) \
    M(UInt64, max_age_to_lower_base_at_min_size, 3600, "", 0) \
    M(UInt64, max_age_to_lower_base_at_max_size, 30 * 86400, "", 0) \
\
    M(UInt64, min_parts_to_lower_base, 10, "", 0) \
    M(UInt64, max_parts_to_lower_base, 50, "", 0) \
\
    /** Add this to size before all calculations. It means: merging even very small parts has it's fixed cost.*/ \
    M(UInt64, size_fixed_cost_to_add, 5 * 1024 * 1024, "", 0) \
\
    /** Heuristic:*/ \
    /** Make some preference for ranges, that sum_size is like (in terms of ratio) to part previous at left.*/ \
    M(Bool, enable_heuristic_to_align_parts, true, "", 0) \
    M(Float, heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part, 0.9, "", 0) \
    M(Float, heuristic_to_align_parts_max_absolute_difference_in_powers_of_two, 0.5, "", 0) \
    M(Float, heuristic_to_align_parts_max_score_adjustment, 0.75, "", 0) \
\
    /** Heuristic:*/ \
    /** From right side of range, remove all parts, that size is less than specified ratio of sum_size.*/ \
    M(Bool, enable_heuristic_to_remove_small_parts_at_right, true, "", 0) \
    M(Float, heuristic_to_remove_small_parts_at_right_max_ratio, 0.01, "", 0) \
\
    /** For batch select mode.*/ \
    /** Currently, only part-merger tool use thesis options.*/ \
    M(Bool, enable_batch_select, false, "", 0) \
    M(UInt64, max_rows_to_merge_at_once, 30000000, "", 0)

DECLARE_SETTINGS_TRAITS(SimpleMergeSelectorSettingsTraits, LIST_OF_SIMPLE_MERGE_SELECTOR_SETTINGS)


class SimpleMergeSelectorSettings : public BaseSettings<SimpleMergeSelectorSettingsTraits>
{
public:
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config);
};


class SimpleMergeSelector final : public IMergeSelector
{
public:
    using Settings = SimpleMergeSelectorSettings;

    explicit SimpleMergeSelector(const Settings & settings_) : settings(settings_)
    {
    }

    PartsRange
    select(const PartsRanges & parts_ranges, const size_t max_total_size_to_merge, MergeScheduler * merge_scheduler = nullptr) override;

    /**
     * Implement multi-merge-select in a very naive way.
     *
     * When 'enable_batch_select' is enabled, this function chooses
     * as many candidates for merge from each partition as possible.
     * Will fallback to single-select otherwise.
     */
    virtual PartsRanges
    selectMulti(const PartsRanges & partitions, size_t max_total_size_to_merge, MergeScheduler * merge_scheduler) override;

private:
    const Settings settings;
};

}
