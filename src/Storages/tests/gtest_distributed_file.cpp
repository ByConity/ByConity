#include <cstddef>
#include <string>
#include <Storages/DistributedDataClient.h>
#include <Storages/DistributedDataService.h>
#include <gtest/gtest.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/MapConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include "common/types.h"
#include <Common/Brpc/BrpcApplication.h>
#include <Common/PODArray.h>
#include <Common/tests/gtest_utils.h>

using namespace DB;

class DistributedFileTest : public ::testing::Test
{
public:
    static brpc::Server server;
    static DistributedDataService file_service;
    static void SetUpTestSuite()
    {
        try
        {
            UnitTest::initLogger("Fatal");
            Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
            BrpcApplication::getInstance().initialize(*map_config);

            if (server.AddService(&file_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            {
                throw Exception("Fail to add service", ErrorCodes::LOGICAL_ERROR);
            }

            // Start the server.
            brpc::ServerOptions options;
            options.idle_timeout_sec = -1;
            if (server.Start(22345, &options) != 0)
            {
                throw Exception("Fail to start Server", ErrorCodes::LOGICAL_ERROR);
            }
            LOG(INFO) << "Started server and wait client connect reading";
        }
        catch (const Exception & e)
        {
            throw Exception(e.message(), DB::ErrorCodes::LOGICAL_ERROR);
        }
    }
    static void TearDownTestCase()
    {
        server.Stop(1000);
        LOG(INFO) << "Closed server and now running status: " << server.IsRunning();
    }
};
brpc::Server DistributedFileTest::server;
DistributedDataService DistributedFileTest::file_service(73400320);

TEST_F(DistributedFileTest, base)
{
    try
    {
        sleep(3);
        auto client = std::make_shared<DistributedDataClient>("127.0.0.1:22345", "/tmp/distributed_file.txt");
        client->createReadStream();

        String data;
        data.resize(100);
        BufferBase::Buffer buffer_total(data.data(), data.data() + 100);
        client->read(0, 26, buffer_total);

        ASSERT_EQ(String(buffer_total.begin(), 26), "abcdefghijklmnopqrstuvwxyz");
    }
    catch (const Exception & e)
    {
        throw Exception(e.message(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}

/** bench remote file read performance **/
struct PerfStatistics
{
    PerfStatistics(int threads_, int length_)
        : thread(threads_)
        , length(length_)
        , total_count(0)
        , total_op_time(0)
        , min_op_time(std::numeric_limits<size_t>::max())
        , max_op_time(0)
    {
    }

    void update(size_t time_micro_sec)
    {
        ++total_count;
        total_op_time += time_micro_sec;

        min_op_time = std::min(min_op_time, time_micro_sec);
        max_op_time = std::max(max_op_time, time_micro_sec);
    }

    void aggregate(const PerfStatistics & rhs)
    {
        total_count += rhs.total_count;
        total_op_time += rhs.total_op_time;
        min_op_time = std::min(min_op_time, rhs.min_op_time);
        max_op_time = std::max(max_op_time, rhs.max_op_time);
    }

    std::string str() const
    {
        return "Threads: " + std::to_string(thread) + ", length: " + std::to_string(length) + ", Count: " + std::to_string(total_count)
            + ", TotalTime: " + std::to_string(total_op_time) + ", MinOp: " + std::to_string(min_op_time)
            + ", MaxOp: " + std::to_string(max_op_time) + ", AvgOp: " + std::to_string(total_op_time / total_count);
    }

    int thread;
    int length;

    size_t total_count;
    size_t total_op_time;
    size_t min_op_time;
    size_t max_op_time;
};


std::vector<PerfStatistics> readMultiThread(int thread_num, size_t op_count)
{
    auto length = 1024 * 1024;
    std::vector<PerfStatistics> worker_statistics(thread_num, {thread_num, length});
    auto read_worker = [&worker_statistics, op_count, length](int idx) {
        PerfStatistics & statistics = worker_statistics[idx];

        std::default_random_engine re;
        std::normal_distribution<double> dist_offset(2000000000, 100000000);
        std::normal_distribution<double> dist_length(1024 * 1024, 100);

        Stopwatch watch;

        for (size_t i = 0; i < op_count; i++)
        {
            UInt64 offset = static_cast<UInt64>(dist_offset(re));
            auto client = std::make_shared<DistributedDataClient>("127.0.0.1:22345", "/tmp/unit_tests_dbms.bin");
            client->createReadStream();
            String data;
            data.resize(length);
            BufferBase::Buffer buffer_total(data.data(), data.data() + length);
            watch.restart();
            client->read(offset, length, buffer_total);
            statistics.update(watch.elapsedMilliseconds());
            if (idx == 0)
                std::cout << idx << " : " << i << std::endl;
        }
    };

    std::vector<std::thread> workers;
    workers.reserve(thread_num);
    for (int i = 0; i < thread_num; i++)
    {
        workers.emplace_back(read_worker, i);
    }

    for (auto & worker : workers)
    {
        worker.join();
    }

    return worker_statistics;
}


//todo(jiashuo) Just test, delete it before merge&release
/*TEST_F(DistributedFileTest, bench)
{
    sleep(3);
    auto statistics = readMultiThread(64, 1000);
    for (const auto & s : statistics)
    {
        std::cout << s.str() << std::endl;
    }
}*/
