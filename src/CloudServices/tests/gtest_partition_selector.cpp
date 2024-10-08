#include <CloudServices/CnchBGThreadPartitionSelector.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>

#include <gtest/gtest.h>

#include <iostream>

using namespace DB;
using InitState = MergeTreeBgTaskStatistics::InitializeState;

struct PartitionStat
{
    String partition_id;
    UInt64 inserted_parts;
    UInt64 merged_parts;
    UInt64 removed_parts;
    time_t time_before_now;
    /// -1 means not be postponed
    Int64 postponed_time_before_now;
};

struct TestData
{
    StorageID storage_id = StorageID("db", "table");
    MergeTreeBgTaskStatisticsPtr bg_task_stats = nullptr;
    std::shared_ptr<CnchBGThreadPartitionSelector> partition_selector = nullptr;
    CnchBGThreadPartitionSelector::RoundRobinState round_robin_state{};
    std::unordered_map<String, time_t> postponed_partitions{};
    Strings all_partitions{};
    std::function<void ()> get_all_partition_hook{};
};

static constexpr UInt64 PART_SIZE = 100 * 1024 * 1024;

void disableDebugLogs()
{
    int info_log_level = Poco::Logger::parseLevel("information");
    Poco::Logger::get("PartitionSelector").setLevel(info_log_level);
}

void rebuildPartitionSelector(TestData & test_data)
{
    auto get_all_partition_callback = [&]() {
        test_data.bg_task_stats->fillMissingPartitions(test_data.all_partitions);
        test_data.get_all_partition_hook();
        return test_data.all_partitions;
    };
    test_data.partition_selector = std::make_shared<CnchBGThreadPartitionSelector>(
        test_data.storage_id,
        test_data.bg_task_stats,
        test_data.postponed_partitions,
        get_all_partition_callback);
}

void initTestData(
    TestData & test_data,
    std::vector<PartitionStat> partitions_stats,
    std::vector<String> partitions_with_no_stats,
    std::function<void ()> get_all_partition_hook,
    InitState state = InitState::InitializeSucceed)
{
    time_t now = time(nullptr);
    test_data.round_robin_state = {now, 0};
    test_data.postponed_partitions.clear();

    test_data.bg_task_stats = std::make_shared<MergeTreeBgTaskStatistics>(test_data.storage_id);
    test_data.bg_task_stats->setInitializeState(state);

    NameSet partitions_set;
    for (const auto & p_stat: partitions_stats)
    {
        test_data.bg_task_stats->addInsertedParts(p_stat.partition_id, p_stat.inserted_parts, p_stat.inserted_parts * PART_SIZE, now - p_stat.time_before_now);
        test_data.bg_task_stats->addMergedParts(p_stat.partition_id, p_stat.merged_parts, p_stat.merged_parts * PART_SIZE, now - p_stat.time_before_now);
        test_data.bg_task_stats->addRemovedParts(p_stat.partition_id, p_stat.removed_parts);
        partitions_set.insert(p_stat.partition_id);
        if (p_stat.postponed_time_before_now >= 0)
            test_data.postponed_partitions[p_stat.partition_id] = now - p_stat.postponed_time_before_now;
    }
    partitions_set.insert(partitions_with_no_stats.begin(), partitions_with_no_stats.end());
    test_data.all_partitions = {partitions_set.begin(), partitions_set.end()};
    test_data.get_all_partition_hook = get_all_partition_hook;

    rebuildPartitionSelector(test_data);
}

bool checkSelectedPartitions(const NameOrderedSet & res, const NameOrderedSet & expected, bool print = true)
{
    bool equal = (res == expected);
    if (print && !equal)
        std::cout << fmt::format("res is [{}] and expect [{}].", fmt::join(res, ", "), fmt::join(expected, ", ")) << std::endl;
    return equal;
}

bool checkSelectedPartitions(const Strings & res, const NameOrderedSet & expected, bool print = true)
{
    return checkSelectedPartitions(NameOrderedSet{res.begin(), res.end()}, expected, print);
}

TEST(TestPartitionSelector, RoundRobinMerge)
{
    UInt64 get_all_partitions_times = 0;
    TestData data;
    Strings res;

    disableDebugLogs();
    auto get_all_partitions_hook = [&get_all_partitions_times] { get_all_partitions_times++; };

    /// Will not triger round robin
    initTestData(data, {{"p1", 1, 0, 0, 0, -1}}, {}, get_all_partitions_hook);
    res = data.partition_selector->selectForMerge(1, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 0); EXPECT_EQ(res.size(), 1);

    /// Some cases will trigger round robin strategy:
    /// 1. bg_task_stats is not initialized (InitializeFailed, Initializing, Uninitialized)
    initTestData(data, {{"p1", 1, 0, 0, 0, -1}}, {}, get_all_partitions_hook, InitState::InitializeFailed);
    res = data.partition_selector->selectForMerge(1, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 1);

    initTestData(data, {{"p1", 1, 0, 0, 0, -1}}, {}, get_all_partitions_hook, InitState::Initializing);
    res = data.partition_selector->selectForMerge(1, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 2); EXPECT_EQ(res.size(), 1);

    initTestData(data, {{"p1", 1, 0, 0, 0, -1}}, {}, get_all_partitions_hook, InitState::Uninitialized);
    res = data.partition_selector->selectForMerge(1, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 3); EXPECT_EQ(res.size(), 1);

    /// 2. estimator size == 0
    initTestData(data, {}, {}, get_all_partitions_hook);
    res = data.partition_selector->selectForMerge(1, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 4); EXPECT_EQ(res.size(), 0);

    initTestData(data, {}, {"p1"}, get_all_partitions_hook);
    res = data.partition_selector->selectForMerge(2, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 5); EXPECT_EQ(res.size(), 1);

    /// 3. iterval > round_robin_interval
    initTestData(data, {{"p1", 1, 0, 0, 0, -1}}, {}, get_all_partitions_hook);
    data.round_robin_state.last_time_sec = time(nullptr) - 100;
    res = data.partition_selector->selectForMerge(1, 99, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 6); EXPECT_EQ(res.size(), 1);
}

TEST(TestPartitionSelector, Merge)
{
    UInt64 get_all_partitions_times = 0;
    TestData data;
    Strings res;

    disableDebugLogs();
    auto get_all_partitions_hook = [&get_all_partitions_times] { get_all_partitions_times++; };

    /// Will not triger round robin and select 2 partitions every time.
    initTestData(data, {{"p1", 100, 100, 0, 0, -1}, {"p2", 100, 100, 0, 0, -1}}, {}, get_all_partitions_hook);
    for (UInt64 i = 0; i < 100UL; i++) 
    {
        res = data.partition_selector->selectForMerge(2, 10000, data.round_robin_state, false);
        EXPECT_EQ(get_all_partitions_times, 0); EXPECT_EQ(res.size(), 2);

        time_t now = time(nullptr);
        data.bg_task_stats->addInsertedParts("p1", 100, 100 * PART_SIZE, now);
        data.bg_task_stats->addMergedParts("p1", 50, 50 * PART_SIZE, now);
        data.bg_task_stats->addInsertedParts("p2", 100, 100 * PART_SIZE, now);
        data.bg_task_stats->addMergedParts("p2", 150, 150 * PART_SIZE, now);
        rebuildPartitionSelector(data);
    }

    /// Will select unknown partitions after round robin
    initTestData(data, {}, {"p1", "p2"}, get_all_partitions_hook);
    res = data.partition_selector->selectForMerge(1, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 1);
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(2, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 2);

    /// Will select most real time partitions and slowest partitions
    initTestData(data, {{"p1", 10, 10, 0, 0, -1}, {"p2", 10, 10, 0, 1, -1}, {"p3", 10, 0, 0, 2, -1}}, {"p4"}, get_all_partitions_hook);
    res = data.partition_selector->selectForMerge(1, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 1); EXPECT_TRUE(checkSelectedPartitions(res, {"p1"}));
    /// 1 real time partition p1 and 1 slowest partition p3
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(2, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 2); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p3"}));
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(2, 10000, data.round_robin_state, true);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 2); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2"}));
    /// 2 real time partition p1, p2 and 1 slowest partition p3
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(3, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 3); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3"}));
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(3, 10000, data.round_robin_state, true);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 3); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3"}));
    /// Won't select unknown partition p4
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(4, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 3); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3"}));
    /// After round robin, unknown partition p4 can be selected
    data.round_robin_state.last_time_sec = time(nullptr) - 10001;
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(4, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 2); EXPECT_EQ(res.size(), 4); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3", "p4"}));
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(4, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 2); EXPECT_EQ(res.size(), 4); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3", "p4"}));
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForMerge(5, 10000, data.round_robin_state, false);
    EXPECT_EQ(get_all_partitions_times, 2); EXPECT_EQ(res.size(), 4); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3", "p4"}));

    /// After round robin, all unknown partitions will be selected randomly
    initTestData(data, {}, {"p1", "p2", "p3", "p4", "p5", "p6"}, get_all_partitions_hook);
    data.partition_selector->selectForMerge(3, 10000, data.round_robin_state, false);
    NameOrderedSet selected_partitions;
    for (int i = 0; i < 20; i++)
    {
        rebuildPartitionSelector(data);
        res = data.partition_selector->selectForMerge(3, 10000, data.round_robin_state, false);
        selected_partitions.insert(res.begin(), res.end());
    }
    EXPECT_EQ(get_all_partitions_times, 3); EXPECT_TRUE(checkSelectedPartitions(selected_partitions, {"p1", "p2", "p3", "p4", "p5", "p6"}));
}

TEST(TestPartitionSelector, RoundRobinGC)
{
    UInt64 get_all_partitions_times = 0;
    TestData data;
    Strings res;

    disableDebugLogs();
    auto get_all_partitions_hook = [&get_all_partitions_times] { get_all_partitions_times++; };

    /// Will not triger round robin
    initTestData(data, {{"p1", 1, 1, 0, 0, -1}}, {}, get_all_partitions_hook);
    res = data.partition_selector->selectForGC(1, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 0);
    EXPECT_EQ(res.size(), 1);

    /// Some cases will trigger round robin strategy:
    /// 1. bg_task_stats is not initialized (InitializeFailed, Initializing, Uninitialized)
    initTestData(data, {{"p1", 1, 1, 0, 0, -1}}, {}, get_all_partitions_hook, InitState::InitializeFailed);
    res = data.partition_selector->selectForGC(1, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 1);
    EXPECT_EQ(res.size(), 1);

    initTestData(data, {{"p1", 1, 1, 0, 0, -1}}, {}, get_all_partitions_hook, InitState::Initializing);
    res = data.partition_selector->selectForGC(1, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 2);
    EXPECT_EQ(res.size(), 1);

    initTestData(data, {{"p1", 1, 1, 0, 0, -1}}, {}, get_all_partitions_hook, InitState::Uninitialized);
    res = data.partition_selector->selectForGC(1, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 3);
    EXPECT_EQ(res.size(), 1);

    /// 2. estimator size == 0
    initTestData(data, {}, {}, get_all_partitions_hook);
    res = data.partition_selector->selectForGC(1, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 4);
    EXPECT_EQ(res.size(), 0);

    initTestData(data, {}, {"p1"}, get_all_partitions_hook);
    res = data.partition_selector->selectForGC(2, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 5);
    EXPECT_EQ(res.size(), 1);

    /// 3. iterval > round_robin_interval
    initTestData(data, {{"p1", 1, 1, 0, 0, -1}}, {}, get_all_partitions_hook);
    data.round_robin_state.last_time_sec = time(nullptr) - 100;
    res = data.partition_selector->selectForGC(1, 99, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 6);
    EXPECT_EQ(res.size(), 1);
}

TEST(TestPartitionSelector, GC)
{
    UInt64 get_all_partitions_times = 0;
    TestData data;
    Strings res;

    disableDebugLogs();
    auto get_all_partitions_hook = [&get_all_partitions_times] { get_all_partitions_times++; };

    /// Will not triger round robin and select 2 partitions every time.
    initTestData(data, {{"p1", 0, 100, 100, 0, -1}, {"p2", 0, 100, 100, 0, -1}}, {}, get_all_partitions_hook);
    for (UInt64 i = 0; i < 100UL; i++) 
    {
        res = data.partition_selector->selectForGC(2, 10000, data.round_robin_state);
        EXPECT_EQ(get_all_partitions_times, 0); EXPECT_EQ(res.size(), 2);

        time_t now = time(nullptr);
        data.bg_task_stats->addMergedParts("p1", 100, 100 * PART_SIZE, now);
        data.bg_task_stats->addRemovedParts("p1", 50);
        data.bg_task_stats->addMergedParts("p2", 100, 100 * PART_SIZE, now);
        data.bg_task_stats->addRemovedParts("p2", 150);
        rebuildPartitionSelector(data);
    }

    /// Will select unknown partitions after round robin
    initTestData(data, {}, {"p1", "p2"}, get_all_partitions_hook);
    res = data.partition_selector->selectForGC(1, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 1);
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForGC(2, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 2);

    /// Will select most real time partitions and slowest partitions
    initTestData(data, {{"p1", 0, 10, 0, 0, -1}, {"p2", 0, 10, 5, 0, -1}, {"p3", 0, 10, 10, 0, -1}}, {"p4"}, get_all_partitions_hook);
    res = data.partition_selector->selectForGC(1, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 1); EXPECT_TRUE(checkSelectedPartitions(res, {"p1"}));
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForGC(2, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 2); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2"}));
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForGC(3, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 3); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3"}));
    /// Won't select unknown partition p4
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForGC(4, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 1); EXPECT_EQ(res.size(), 3); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3"}));
    /// After round robin, unknown partition p4 can be selected
    data.round_robin_state.last_time_sec = time(nullptr) - 10001;
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForGC(4, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 2); EXPECT_EQ(res.size(), 4); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3", "p4"}));
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForGC(4, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 2); EXPECT_EQ(res.size(), 4); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3", "p4"}));
    rebuildPartitionSelector(data);
    res = data.partition_selector->selectForGC(5, 10000, data.round_robin_state);
    EXPECT_EQ(get_all_partitions_times, 2); EXPECT_EQ(res.size(), 4); EXPECT_TRUE(checkSelectedPartitions(res, {"p1", "p2", "p3", "p4"}));

    /// After round robin, all unknown partitions will be selected randomly
    initTestData(data, {}, {"p1", "p2", "p3", "p4", "p5", "p6"}, get_all_partitions_hook);
    data.partition_selector->selectForGC(1, 10000, data.round_robin_state);
    NameOrderedSet selected_partitions;
    for (int i = 0; i < 20; i++)
    {
        rebuildPartitionSelector(data);
        res = data.partition_selector->selectForGC(3, 10000, data.round_robin_state);
        selected_partitions.insert(res.begin(), res.end());
    }
    EXPECT_EQ(get_all_partitions_times, 3); EXPECT_TRUE(checkSelectedPartitions(selected_partitions, {"p1", "p2", "p3", "p4", "p5", "p6"}));
}
