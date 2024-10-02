#include <algorithm>
#include <sstream>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include <Interpreters/NodeSelector.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <Common/HostWithPorts.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <common/types.h>

std::string formatDistributedBucketResultMap(const DB::NodeSelectorResult & result)
{
    std::stringstream ss;
    for (const auto & e : result.buckets_on_workers)
        ss << "{'" << e.first.getHostName() << "':'["
           << boost::join(
                  e.second | boost::adaptors::transformed([](const std::set<Int64> & b) {
                      return "(" + boost::join(b | boost::adaptors::transformed([](Int64 bb) { return std::to_string(bb); }), ",") + ")";
                  }),
                  ",")
           << "]'}";
    return ss.str();
}

void checkDistributeBucketResultMap(
    DB::NodeSelectorResult & expected_result, DB::NodeSelectorResult & result, const std::vector<DB::AddressInfo> & hosts)
{
    for (const auto & host : hosts)
    {
        auto range = result.buckets_on_workers[host];
        ASSERT_EQ(expected_result.buckets_on_workers[host].size(), result.buckets_on_workers[host].size())
            << "host is " << host.getHostName() << " result is " << formatDistributedBucketResultMap(result);
        size_t index = 0;
        for (const auto & iter : range)
        {
            EXPECT_THAT(iter, ::testing::ContainerEq(expected_result.buckets_on_workers[host][index]))
                << "host is " << host.getHostName() << " result is " << result.toString();
            index++;
        }
    }
    for (size_t i = 0; i < result.worker_nodes.size(); i++)
    {
        ASSERT_EQ(result.worker_nodes[i].address.getHostName(), expected_result.worker_nodes[i].address.getHostName())
            << " result is" << result.toString();
        ASSERT_EQ(result.worker_nodes[i].id, expected_result.worker_nodes[i].id);
    }
}

TEST(NodeSelectorTest, divideSourceTaskByBucketTestCase1)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1", .rows = 3, .part_num = 3, .bucket_groups = {{0, {0}}, {2, {2}}, {4, {4}}}};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 3, .part_num = 3, .bucket_groups = {{1, {1}}, {3, {3}}, {5, {5}}}};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.buckets_on_workers.insert({hosts[0], {{0}, {2, 4}}});
    expected_result.buckets_on_workers.insert({hosts[1], {{1}, {3, 5}}});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    size_t rows_sum = 6;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByBucket(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributeBucketResultMap(expected_result, result, hosts);
}

/// uneven distribution of buckets
TEST(NodeSelectorTest, divideSourceTaskByBucketCase2)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{
        .worker_id = "1",
        .rows = 7,
        .part_num = 7,
        .bucket_groups = {{0, {0}}, {2, {2}}, {4, {4}}, {6, {6}}, {8, {8}}, {10, {10}}, {12, {12}}}};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 2, .part_num = 2, .bucket_groups = {{1, {1}}, {3, {3}}}};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.buckets_on_workers.insert({hosts[0], {{0, 2}, {4, 6}, {8, 10, 12}}});
    expected_result.buckets_on_workers.insert({hosts[1], {{1, 3}}});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    size_t rows_sum = 9;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByBucket(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributeBucketResultMap(expected_result, result, hosts);
}

/// buckets less than parallel size
TEST(NodeSelectorTest, divideSourceTaskByBucketCase3)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1", .rows = 2, .part_num = 2, .bucket_groups = {{0, {0}}, {4, {4}}}};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 1, .part_num = 1, .bucket_groups = {{3, {3}}}};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.buckets_on_workers.insert({hosts[0], {{0}, {4}}});
    expected_result.buckets_on_workers.insert({hosts[1], {{3}, {-1}}});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    size_t rows_sum = 3;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByBucket(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributeBucketResultMap(expected_result, result, hosts);
}

/// empty table
TEST(NodeSelectorTest, divideSourceTaskByBucketCase4)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1"};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2"};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.buckets_on_workers.insert({hosts[0], {{-1}, {-1}}});
    expected_result.buckets_on_workers.insert({hosts[1], {{-1}, {-1}}});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    size_t rows_sum = 0;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByBucket(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributeBucketResultMap(expected_result, result, hosts);
}

/// two tables
TEST(NodeSelectorTest, divideSourceTaskByBucketCase5)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    // first table containing buckets [0, 1, 2, 3, 5], max bucket number is 8
    // second table containing buckets [0, 1, 3], max bucket number is 4
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1", .rows = 3, .part_num = 3, .bucket_groups = {{0, {0}}, {2, {2}}}};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 5, .part_num = 5, .bucket_groups = {{1, {1, 5}}, {3, {3}}}};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.buckets_on_workers.insert({hosts[0], {{0, 2}}});
    expected_result.buckets_on_workers.insert({hosts[1], {{1, 5}, {3}, {-1}}});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    size_t rows_sum = 8;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByBucket(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributeBucketResultMap(expected_result, result, hosts);
}

/// one big part
TEST(NodeSelectorTest, divideSourceTaskByBucketCase6)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1", .rows = 3, .part_num = 3, .bucket_groups = {{0, {0}}, {2, {2}}}};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 9, .part_num = 9, .bucket_groups = {{1, {1, 5}}}};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.buckets_on_workers.insert({hosts[0], {{0}, {2}}});
    expected_result.buckets_on_workers.insert({hosts[1], {{1, 5}, {-1}}});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    size_t rows_sum = 12;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByBucket(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributeBucketResultMap(expected_result, result, hosts);
}

std::string formatDistributedPartsResultMap(const std::multimap<size_t, size_t> & map)
{
    std::stringstream ss;
    for (const auto & e : map)
        ss << "{'" << e.first << "':'[" << e.second << "]'}";
    return ss.str();
}

void checkDistributePartsResultMap(
    DB::NodeSelectorResult & expected_result, DB::NodeSelectorResult & result, const std::vector<DB::AddressInfo> & hosts)
{
    for (const auto & host : hosts)
    {
        ASSERT_EQ(expected_result.source_task_count_on_workers[host], result.source_task_count_on_workers[host])
            << "host is " << host.toShortString() << " result is " << result.toString();
    }
    for (size_t i = 0; i < result.worker_nodes.size(); i++)
    {
        ASSERT_EQ(result.worker_nodes[i].address.getHostName(), expected_result.worker_nodes[i].address.getHostName())
            << " result is" << result.toString();
        ASSERT_EQ(result.worker_nodes[i].id, expected_result.worker_nodes[i].id) << " result is" << result.toString();
    }
}

TEST(NodeSelectorTest, divideTaskByPartTestCase1)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1", .rows = 2, .part_num = 2};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 3, .part_num = 3};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.source_task_count_on_workers.insert({hosts[0], 1});
    expected_result.source_task_count_on_workers.insert({hosts[1], 3});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    size_t rows_sum = 5;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByPart(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributePartsResultMap(expected_result, result, hosts);
}

/// uneven case
TEST(NodeSelectorTest, divideTaskByPartTestCase2)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1", .rows = 7, .part_num = 7};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 2, .part_num = 2};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.source_task_count_on_workers.insert({hosts[0], 3});
    expected_result.source_task_count_on_workers.insert({hosts[1], 1});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    size_t rows_sum = 9;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByPart(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributePartsResultMap(expected_result, result, hosts);
}

/// size less than parallel_size
TEST(NodeSelectorTest, divideTaskByPartTestCase3)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1", .rows = 1, .part_num = 1};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 2, .part_num = 2};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.source_task_count_on_workers.insert({hosts[0], 1});
    expected_result.source_task_count_on_workers.insert({hosts[1], 3});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    size_t rows_sum = 3;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByPart(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributePartsResultMap(expected_result, result, hosts);
}

/// empty table
TEST(NodeSelectorTest, divideTaskByPartTestCase4)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1"};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2"};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.source_task_count_on_workers.insert({hosts[0], 2});
    expected_result.source_task_count_on_workers.insert({hosts[1], 2});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    size_t rows_sum = 0;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByPart(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributePartsResultMap(expected_result, result, hosts);
}

/// one big part
TEST(NodeSelectorTest, divideTaskByPartTestCase5)
{
    std::vector<DB::AddressInfo> hosts{DB::AddressInfo("host1", 0, "", "", 0), DB::AddressInfo("host2", 0, "", "", 0)};
    std::unordered_map<DB::AddressInfo, DB::SourceTaskPayloadOnWorker, DB::AddressInfo::Hash> payload_on_worker;
    auto p1 = DB::SourceTaskPayloadOnWorker{.worker_id = "1", .rows = 3, .part_num = 3};
    payload_on_worker.insert({hosts[0], std::move(p1)});
    auto p2 = DB::SourceTaskPayloadOnWorker{.worker_id = "2", .rows = 9, .part_num = 1};
    payload_on_worker.insert({hosts[1], std::move(p2)});
    DB::NodeSelectorResult expected_result;
    expected_result.source_task_count_on_workers.insert({hosts[0], 2});
    expected_result.source_task_count_on_workers.insert({hosts[1], 2});
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[1], Coordination::NodeType::Remote, "2"));
    expected_result.worker_nodes.emplace_back(DB::WorkerNode(hosts[0], Coordination::NodeType::Remote, "1"));
    size_t rows_sum = 12;
    size_t parallel_size = 4;

    DB::NodeSelectorResult result;
    Coordination::divideSourceTaskByPart(payload_on_worker, rows_sum, parallel_size, result);

    checkDistributePartsResultMap(expected_result, result, hosts);
}

std::string printPartitionCoalescingExpectedResult(const std::map<DB::PlanSegmentInstanceId, std::vector<UInt32>> & result)
{
    std::stringstream ss;
    for (const auto & iter : result)
    {
        ss << iter.first.toString() << ":["
           << boost::join(iter.second | boost::adaptors::transformed([](UInt32 b) { return std::to_string(b); }), ",") << "]"
           << "\t";
    }
    return ss.str();
}

// 2, 3, 4 ===coalesced to==> 5, 4
TEST(NodeSelectorTest, PartitionCoalescingTestCase1)
{
    UInt32 segment_id = 1;
    size_t disk_shuffle_advisory_partition_size = 3;
    std::vector<size_t> normal_partitions = {2, 3, 4};
    size_t broadcast_partition = 0;
    auto result
        = DB::NodeSelector::coalescePartitions(segment_id, disk_shuffle_advisory_partition_size, broadcast_partition, normal_partitions);
    ASSERT_EQ(result.size(), 2) << printPartitionCoalescingExpectedResult(result);
    std::map<DB::PlanSegmentInstanceId, std::vector<UInt32>> expected_result
        = {{DB::PlanSegmentInstanceId{1, 0}, {0, 1}}, {DB::PlanSegmentInstanceId{1, 1}, {2}}};
    EXPECT_THAT(result, ::testing::ContainerEq(expected_result));
}

// 1, 1, 2, 2 ===coalesced to==> 2, 2, 2
// broadcast partition when coalesced will only read once
TEST(NodeSelectorTest, PartitionCoalescingTestCase2)
{
    UInt32 segment_id = 1;
    size_t disk_shuffle_advisory_partition_size = 3;
    std::vector<size_t> normal_partitions = {1, 1, 2, 2};
    size_t broadcast_partition = 1;
    auto result
        = DB::NodeSelector::coalescePartitions(segment_id, disk_shuffle_advisory_partition_size, broadcast_partition, normal_partitions);
    ASSERT_EQ(result.size(), 3) << printPartitionCoalescingExpectedResult(result);
    std::map<DB::PlanSegmentInstanceId, std::vector<UInt32>> expected_result
        = {{DB::PlanSegmentInstanceId{1, 0}, {0, 1}}, {DB::PlanSegmentInstanceId{1, 1}, {2}}, {DB::PlanSegmentInstanceId{1, 2}, {3}}};
    EXPECT_THAT(result, ::testing::ContainerEq(expected_result));
}
