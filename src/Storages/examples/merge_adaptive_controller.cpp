#include <Common/interpolate.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/DanceMergeSelector.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>

#include <iostream>
#include <queue>

using namespace DB;

struct TestParams
{
    UInt64 real_time_test_ms;
    UInt64 historical_test_ms;
    double parts_insert_per_hour;
    double parts_merge_per_hour;
    double select_per_hour;
    UInt64 insert_interval_ms;
    UInt64 select_interval_ms;
    UInt64 part_merge_elapsed_ms;
    UInt64 max_concurrent_merges;
    UInt64 num_workers;
    bool enable_trace;
} params;

enum TestActionType
{
    ACTION_TYPE_NONE,
    ACTION_TYPE_INSERT,
    ACTION_TYPE_MERGE_SELECT,
    ACTION_TYPE_MERGE_COMPLETED,
};

struct TestAction
{
    UInt64 ts; /// in milliseconds
    TestActionType type;

    IMergeSelector::PartsRange merge_parts{};
    IMergeSelector::Part future_part{};

    /// need to order by ts assending
    bool operator < (const TestAction & rhs) const { return this->ts > rhs.ts; }
};


constexpr const UInt64 SECOND_TO_MS = 1000UL;
constexpr const UInt64 MINUTE_TO_MS = 60UL * SECOND_TO_MS;
constexpr const UInt64 HOUR_TO_MS = 60UL * MINUTE_TO_MS;
constexpr const UInt64 DAY_TO_MS = 24UL * HOUR_TO_MS;

constexpr const UInt64 SAMPLE_INTERVAL_MS = 1 * MINUTE_TO_MS;

const UUID STORAGE_UUID = UUIDHelpers::generateV4();
const String PARTITION_ID = "all";

const UInt64 LEVEL_0_PART_ROWS = 10000;
const UInt64 LEVEL_0_PART_SIZE = 100 * 1024 * 1024; // 10MB

struct TestState
{
    IMergeSelector::PartsRanges all_partitions{{}};
    IMergeSelector::PartsRange future_merging_parts{};
    std::priority_queue<TestAction> actions_queue{};

    UInt64 milliseconds{0};
    UInt64 seconds{0};
    UInt64 block_id{0};
    UInt64 concurrent_merges{0};
    UInt64 concurrent_merging_parts{0};
    UInt64 merged_parts{0};
    UInt64 inserted{0};
    UInt64 merged{0};
    UInt64 inserted_bytes{0};
    UInt64 merged_bytes{0};
    unsigned max_level{0};

    UInt64 sampled_parts_count{0};
    UInt64 sample_count{0};

    std::shared_ptr<MergeTreeBgTaskStatistics> bg_task_stats;

    void reset()
    {
        *this = TestState{};
        bg_task_stats = std::make_shared<MergeTreeBgTaskStatistics>(STORAGE_UUID);
        bg_task_stats->setInitializeState(MergeTreeBgTaskStatistics::InitializeState::InitializeSucceed);
    }

} current_stat{};

void updatePartsAge(UInt64 curr_ms, UInt64 last_ms)
{
    if (curr_ms > last_ms)
    {
        time_t skip_s = (curr_ms - last_ms) / SECOND_TO_MS;
        for (auto & p: current_stat.all_partitions[0])
            p.age += skip_s;
    }
}

String getPartitionID(const IMergeSelector::Part &)
{
    return PARTITION_ID;
}

static double mapPiecewiseLinearToUnit(double value, double min, double max)
{
    return value <= min ? 0 : (value >= max ? 1 : ((value - min) / (max - min)));
}

/// Like std::min, but treat 0 as infinity
static size_t minValueWithoutZero(size_t a, size_t b)
{
    return a == 0 ? b : (b == 0 ? a : std::min(a, b));
}

IMergeSelector::PartsRange mergeSelect(bool with_adaptive_controller)
{
    DanceMergeSelector::Settings settings;
    settings.select_nonadjacent_parts_allowed = true;
    settings.min_parts_to_merge_base = 3;
    DanceMergeSelector selector(settings);
    selector.debugSetGetPartitionID(getPartitionID);

    if (with_adaptive_controller)
    {
        std::unordered_map<String, std::vector<UInt64> > future_part_rows {{PARTITION_ID, {}}};
        for (const auto & p: current_stat.future_merging_parts)
            future_part_rows[PARTITION_ID].emplace_back(p.rows);

        auto adaptive_controller = std::make_shared<MergeSelectorAdaptiveController>(
            /*is_bucket_table_*/ false,
            params.num_workers,
            settings.max_parts_to_merge_base.value);
        adaptive_controller->debugSetGetPartitionID(getPartitionID);
        adaptive_controller->debugSetNow(current_stat.seconds);
        adaptive_controller->init(current_stat.bg_task_stats, current_stat.all_partitions, future_part_rows);
        
        /// Some trace logs with adaptive controller
        if (params.enable_trace)
        {
            bool need_control = adaptive_controller->needControlWriteAmplification(PARTITION_ID);
            const auto & [max_parts_, max_rows_] = adaptive_controller->getMaxPartsAndRows(PARTITION_ID);
            size_t max_parts = std::min(max_parts_, settings.max_parts_to_merge_base.value);
            size_t max_rows = minValueWithoutZero(max_rows_, settings.max_total_rows_to_merge);

            std::cout << "Current parts " << current_stat.all_partitions[0].size() << " future parts " << current_stat.future_merging_parts.size()
                << " total rows " << LEVEL_0_PART_ROWS * current_stat.inserted
                << ", max parts rows before (" << settings.max_parts_to_merge_base.value << "," << settings.max_total_rows_to_merge
                << "), output (" << max_parts_ << "," << max_rows_ << "), final (" << max_parts << "," << max_rows << ")" << std::endl;

            if (need_control)
            {
                const auto & [wa, wa_min, wa_max] = adaptive_controller->getWriteAmplification(PARTITION_ID);
                double base = settings.min_parts_to_merge_base;
                if (wa > wa_min)
                {
                    double ratio = mapPiecewiseLinearToUnit(std::log1p(wa), std::log1p(wa_min), std::log1p(wa_max));
                    base = interpolateLinear(base, std::max(base, settings.max_parts_to_merge_base.value / 5.0), ratio);

                    std::cout << std::fixed << std::setprecision(2)
                        << "Control write amplification (" << wa << "," << wa_min << "," << wa_max << "), ratio " << ratio
                        << ", base " << base << std::endl;
                }
            }
        }

        selector.setAdaptiveController(adaptive_controller);
    }

    IMergeSelector::PartsRange selected_parts = selector.select(current_stat.all_partitions, 0);

    /// Some trace logs with merge selector
    if (params.enable_trace && with_adaptive_controller && !selected_parts.empty())
    {
        UInt64 total_rows = 0;
        for (auto & p: selected_parts)
            total_rows += p.rows;

        std::cout << "Selected merge with " << selected_parts.size() << " parts with " << total_rows << " rows.\n";
    }

    return selected_parts;
}

void testImpl(UInt64 start_time, UInt64 final_time, bool have_inserts, bool with_adaptive_controller, UInt64 stop_early = 0)
{
    if (start_time == 0)
    {
        current_stat.actions_queue.push(TestAction{.ts = start_time, .type = ACTION_TYPE_MERGE_SELECT});
        current_stat.actions_queue.push(TestAction{.ts = start_time, .type = ACTION_TYPE_INSERT});
    }
    
    UInt64 start_time_s = start_time / SECOND_TO_MS;

    UInt64 last_sample_time = 0;
    UInt64 unselected_count = 0;
    for (TestAction current_action; current_stat.milliseconds < final_time && !current_stat.actions_queue.empty(); current_stat.actions_queue.pop())
    {
        current_action = current_stat.actions_queue.top();
        updatePartsAge(current_action.ts, current_stat.milliseconds);
        current_stat.milliseconds = current_action.ts;
        current_stat.seconds = start_time_s + current_stat.milliseconds / SECOND_TO_MS;

        if (current_stat.milliseconds > last_sample_time + SAMPLE_INTERVAL_MS)
        {
            current_stat.sampled_parts_count += current_stat.all_partitions[0].size() + current_stat.concurrent_merging_parts;
            current_stat.sample_count++;
        }

        switch (current_action.type)
        {
        case ACTION_TYPE_NONE:
        {
            std::cerr << "Got ACTION_TYPE_NONE at " << current_stat.milliseconds << std::endl;
            return;
        }
        case ACTION_TYPE_INSERT:
        {
            if (!have_inserts)
                break;

            /// TODO(shiyuze): support random rows/size
            IMergeSelector::Part part = IMergeSelector::Part{
                .size = LEVEL_0_PART_SIZE,
                .rows = LEVEL_0_PART_ROWS,
                .age = 0,
                .level = 0,
                .data = reinterpret_cast<const void *>(current_stat.block_id++),
            };

            current_stat.inserted++;
            current_stat.inserted_bytes += part.size;
            current_stat.all_partitions[0].push_back(std::move(part));
            current_stat.bg_task_stats->addInsertedParts(PARTITION_ID, 1, part.size, current_stat.seconds);
            current_stat.actions_queue.push(TestAction{.ts = current_stat.milliseconds + params.insert_interval_ms, .type = ACTION_TYPE_INSERT});
        }
        break;
        case ACTION_TYPE_MERGE_SELECT:
        {
            bool selected = true;
            if (current_stat.concurrent_merges < params.max_concurrent_merges)
            {
                auto selected_range = mergeSelect(with_adaptive_controller);
                selected = !selected_range.empty();
                if (!selected_range.empty())
                {
                    auto merge_completed_action = TestAction{
                        .ts = current_stat.milliseconds + selected_range.size() * params.part_merge_elapsed_ms,
                        .type = ACTION_TYPE_MERGE_COMPLETED,
                        .merge_parts = selected_range,
                        .future_part = {.size = 0, .rows = 0, .age = 0, .level = 0, .data = reinterpret_cast<const void *>(current_stat.block_id++), },
                    };

                    std::unordered_set<const void *> selected_datas;
                    for (auto & p: selected_range)
                    {
                        merge_completed_action.future_part.size += p.size;
                        merge_completed_action.future_part.rows += p.rows;
                        merge_completed_action.future_part.level = std::max(merge_completed_action.future_part.level, p.level + 1);
                        selected_datas.insert(p.data);
                    }

                    current_stat.all_partitions[0].erase(
                        std::remove_if(current_stat.all_partitions[0].begin(), current_stat.all_partitions[0].end(),
                            [&](const IMergeSelector::Part & p) { return selected_datas.count(p.data); }),
                        current_stat.all_partitions[0].end()
                    );

                    current_stat.future_merging_parts.emplace_back(merge_completed_action.future_part);
                    current_stat.actions_queue.push(std::move(merge_completed_action));
                    current_stat.concurrent_merges++;
                    current_stat.concurrent_merging_parts += selected_range.size();
                }
            }
            UInt64 select_interval_ms = selected ? params.select_interval_ms : params.select_interval_ms * 10;
            unselected_count = selected ? 0 : unselected_count + 1;
            current_stat.actions_queue.push(TestAction{.ts = current_stat.milliseconds + select_interval_ms, .type = ACTION_TYPE_MERGE_SELECT});
        }
        break;
        case ACTION_TYPE_MERGE_COMPLETED:
        {
            const auto & part = current_action.future_part;

            UInt64 num_future_parts_before = current_stat.future_merging_parts.size();
            current_stat.future_merging_parts.erase(
                std::remove_if(current_stat.future_merging_parts.begin(), current_stat.future_merging_parts.end(),
                    [&](const IMergeSelector::Part & p) { return p.data == part.data; }),
                current_stat.future_merging_parts.end()
            );
            if (current_stat.future_merging_parts.size() != num_future_parts_before - 1)
                abort();

            current_stat.merged_parts += current_action.merge_parts.size();
            current_stat.merged++;
            current_stat.merged_bytes += part.size;
            current_stat.max_level = std::max(current_stat.max_level, part.level);
            current_stat.bg_task_stats->addMergedParts(PARTITION_ID, current_action.merge_parts.size(), part.size, current_stat.seconds);
            current_stat.all_partitions[0].push_back(std::move(part));

            current_stat.concurrent_merges--;
            current_stat.concurrent_merging_parts -= current_action.merge_parts.size();
        }
        break;
        }

        if (current_stat.concurrent_merges == 0 && stop_early > 0 && unselected_count > stop_early)
        {
            current_stat.actions_queue.pop();
            for (TestAction current_action_; !current_stat.actions_queue.empty(); current_stat.actions_queue.pop())
            {
                current_action_ = current_stat.actions_queue.top();
                if (current_action_.type == ACTION_TYPE_MERGE_COMPLETED)
                    std::copy(current_action_.merge_parts.begin(),  current_action_.merge_parts.end(), std::back_inserter(current_stat.all_partitions[0]));
            }
            current_stat.concurrent_merging_parts = 0;
            break;
        }
    }
}

void testRealtimePartition(bool with_adaptive_controller)
{
    testImpl(0, params.real_time_test_ms, true, with_adaptive_controller, 0);

    std::cout << std::fixed << std::setprecision(2)
        << "Test realtime partition with" << (with_adaptive_controller ? "" : "out") << " adaptive controller\n"
        << "Write amplification: " << (static_cast<double>(current_stat.merged_bytes) / current_stat.inserted_bytes + 1.0)
        << "  Avg parts remains: " << (current_stat.sample_count == 0 ? 0 : static_cast<double>(current_stat.sampled_parts_count/ current_stat.sample_count))
        << "  Avg merged: " << (current_stat.merged == 0 ? 0 : static_cast<double>(current_stat.merged_parts) / current_stat.merged)
        << "  Tree depth: " << current_stat.max_level
        << "  Final parts: " << current_stat.all_partitions[0].size()
        << "\n\n";
}

template<typename T>
T variance(const std::vector<T> &vec) {
    const size_t sz = vec.size();
    if (sz <= 1) {
        return 0.0;
    }

    // Calculate the mean
    const T mean = std::accumulate(vec.begin(), vec.end(), 0.0) / sz;

    // Now calculate the variance
    auto variance_func = [&mean, &sz](T accumulator, const T& val) {
        return accumulator + ((val - mean)*(val - mean) / (sz - 1));
    };

    return std::accumulate(vec.begin(), vec.end(), 0.0, variance_func);
}

void testHistoricalPartition(bool with_adaptive_controller)
{
    UInt64 merged_before = current_stat.merged;
    UInt64 merged_bytes_before = current_stat.merged_bytes;
    testImpl(params.real_time_test_ms, params.real_time_test_ms + params.historical_test_ms, false, with_adaptive_controller, 3000);

    std::sort(current_stat.all_partitions[0].begin(), current_stat.all_partitions[0].end(), [](const IMergeSelector::Part & lhs, const IMergeSelector::Part & rhs)
    {
        return lhs.level > rhs.level;
    });

    std::vector<double> part_rows;
    std::stringstream final_parts_ss;
    for (auto & part: current_stat.all_partitions[0])
    {
        final_parts_ss << " " << part.level;
        part_rows.push_back(part.rows);
    }

    std::cout << std::fixed << std::setprecision(2)
        << "Test historical partition with" << (with_adaptive_controller ? "" : "out") << " adaptive controller\n"
        << "Write amplification: " << (static_cast<double>(current_stat.merged_bytes - merged_bytes_before) / current_stat.inserted_bytes + 1.0)
        << "  Elapsed time: " << static_cast<double>(current_stat.milliseconds - params.real_time_test_ms) / HOUR_TO_MS << " hour.\n"
        << "  Num merges: " << current_stat.merged - merged_before
        << "  Tree depth: " << current_stat.max_level
        << "  Final parts: " << current_stat.all_partitions[0].size() << " ( " << final_parts_ss.str() << " )"
        << "  Standard deviation or rows: " << std::sqrt(variance(part_rows))
        << "\n\n\n";
}


String helpString(const String & cmd)
{
    return cmd + "  parts_insert_per_hour  merge_speed  select_per_hour  max_concurrent_merges  num_workers  [real_time_test_hour  historical_test_hour  enable_trace]";
}

int main(int argc, char ** argv)
{
    if (argc < 6)
    {
        std::cout << helpString(argv[0]) << std::endl;
        return -1;
    }

    params = TestParams{
        .real_time_test_ms = 24 * HOUR_TO_MS,
        .historical_test_ms = 3 * DAY_TO_MS,

        .parts_insert_per_hour = std::stod(argv[1]),
        .parts_merge_per_hour = std::stod(argv[1]) * std::stod(argv[2]),
        .select_per_hour = std::stod(argv[3]),
        .insert_interval_ms = static_cast<UInt64>(3600000.0 / std::stod(argv[1])),
        .select_interval_ms = static_cast<UInt64>(3600000.0 / std::stod(argv[3])),
        .part_merge_elapsed_ms = static_cast<UInt64>(3600000.0 / std::stod(argv[1]) / std::stod(argv[2])),
        .max_concurrent_merges = std::stoul(argv[4]),
        .num_workers = std::stoull(argv[5]),
        .enable_trace = false,
    };
    if (argc > 6)
        params.real_time_test_ms = std::stoull(argv[6]) * HOUR_TO_MS;
    if (argc > 7)
        params.historical_test_ms = std::stoull(argv[7]) * HOUR_TO_MS;
    if (argc > 8)
        params.enable_trace = bool(std::stoull(argv[8]));

    if (!params.insert_interval_ms || !params.select_interval_ms || !params.part_merge_elapsed_ms)
    {
        std::cout << "Too big parts_insert_per_hour, merge_speed or select_per_hour" << std::endl;
        return -1;
    }

    current_stat.reset();
    testRealtimePartition(false);
    testHistoricalPartition(false);

    current_stat.reset();
    testRealtimePartition(true);
    testHistoricalPartition(true);

    return 0;
}
