#include <filesystem>
#include <memory>
#include <ostream>
#include <vector>
#include <unistd.h>
#include <Disks/registerDisks.h>
#include <IO/LimitReadBuffer.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/PartFileDiskCacheSegment.h>
#include <Storages/DistributedDataClient.h>
#include <Storages/DistributedDataService.h>
#include <Storages/RemoteDiskCacheService.h>
#include <gtest/gtest.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/MapConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Brpc/BrpcApplication.h>
#include <Common/PODArray.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include "Disks/IStoragePolicy.h"
#include "IO/ReadBufferFromFile.h"
#include "IO/ReadBufferFromFileBase.h"
#include "Storages/MergeTree/MergeTreeSuffix.h"
#include <Storages/DiskCache/DiskCacheFactory.h>

using namespace DB;

class RemoteCacheTest : public ::testing::Test
{
public:
    static brpc::Server server;
    static std::shared_ptr<Context> ctx;
    static std::vector<std::unique_ptr<RemoteDiskCacheService>> services;
    static constexpr const UInt32 segment_size = 8192;

    static void SetUpTestSuite()
    {
        try
        {
            UnitTest::initLogger();
            Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
            BrpcApplication::getInstance().initialize(*map_config);
            fs::create_directories("./disks/");
            fs::create_directory("./disk_cache/");
            fs::create_directory("./disk_cache_v1/");
            fs::create_directory("/tmp/.test/");
            fs::create_directory("/tmp/.test/disk_cache/");
            fs::create_directory("/tmp/.test/disk_cache_v1/");
            if (!fs::exists("/tmp/.test/distributed_file.txt"))
                fs::copy_file("/tmp/distributed_file.txt", "/tmp/.test/distributed_file.txt");
            if (!fs::exists("./gtest_tmp/distributed_file.txt"))
                fs::copy_file("/tmp/distributed_file.txt", "./gtest_tmp/distributed_file.txt");
            ctx = getContext().context;
            Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration("/tmp/disk_cache_test_new_config.xml");
            ctx->setConfig(config);

            try
            {
                if (ctx->getDisksMap().find("local") == ctx->getDisksMap().end())
                {
                    registerDisks();
                }
            }
            catch (...)
            {
                registerDisks();
            }

            DiskCacheFactory::instance().init(*ctx);

            auto service = std::make_unique<RemoteDiskCacheService>(ctx);
            services.emplace_back(std::move(service));
            if (server.AddService(services.back().get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            {
                throw Exception("Fail to add remote cache service", DB::ErrorCodes::LOGICAL_ERROR);
            }

            // Start the server.
            brpc::ServerOptions options;
            options.idle_timeout_sec = -1;
            if (server.Start(12346, &options) != 0)
            {
                throw Exception("Fail to start remote cache server", DB::ErrorCodes::LOGICAL_ERROR);
            }
            LOG(INFO) << "Started remote cache server and wait client connect read";
        }
        catch (const Exception & e)
        {
            throw Exception(e.message(), DB::ErrorCodes::LOGICAL_ERROR);
        }
    }
    static void TearDownTestCase()
    {
        server.Stop(1000);
        DB::IDiskCache::close();
        DistributedDataService::thread_pool.reset();
        LOG(INFO) << "Closed server and now running status: " << server.IsRunning();
    }
};
std::shared_ptr<Context> RemoteCacheTest::ctx = nullptr;
brpc::Server RemoteCacheTest::server;
std::vector<std::unique_ptr<RemoteDiskCacheService>> RemoteCacheTest::services(1);

TEST_F(RemoteCacheTest, read)
{
    try
    {
        sleep(5);
        auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getDataCache();

        std::shared_ptr<ReadBufferFromFileBase> file = std::make_shared<ReadBufferFromFile>("/tmp/distributed_file.txt");
        file->seek(0);
        LimitReadBuffer segment_value(*file, 26, false);
        auto key = PartFileDiskCacheSegment::getSegmentKey({"test", "test"}, "distributed_file", "", 0, DATA_FILE_EXTENSION);
        disk_cache->set(key, segment_value, 26);

        auto client = std::make_shared<DistributedDataClient>("127.0.0.1:12346", key);
        client->createReadStream();

        ASSERT_TRUE(!client->remote_file_path.empty());

        String data;
        data.resize(26);
        BufferBase::Buffer buffer_total(data.data(), data.data() + 100);
        client->read(0, 26, buffer_total);

        ASSERT_EQ(String(buffer_total.begin(), 26), "abcdefghijklmnopqrstuvwxyz");
        fs::remove(client->remote_file_path);
    }
    catch (const Exception & e)
    {
        throw Exception(e.message(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}

TEST_F(RemoteCacheTest, write)
{
    try
    {
        sleep(5);
        auto client = std::make_shared<DistributedDataClient>("127.0.0.1:12346");
        auto key = PartFileDiskCacheSegment::getSegmentKey(
            {"test_write", "test_write"}, "distributed_file_write", "stream", 1000, DATA_FILE_EXTENSION);
        std::vector<WriteFile> files{{key, "distributed_file.txt", 0, 26}};
        client->write("default", files);
        auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getDataCache();
        sleep(5);
        auto cache = disk_cache->get(key);
        ASSERT_TRUE(cache.first);
        ASSERT_TRUE(!cache.second.empty());
        auto file = cache.first->readFile(cache.second);

        PODArray<char> buffer;
        buffer.resize(26);
        file->seek(0, SEEK_SET);
        file->readStrict(buffer.data(), 26);
        ASSERT_EQ(String(buffer.begin(), 26), "abcdefghijklmnopqrstuvwxyz");
    }
    catch (const Exception & e)
    {
        throw Exception(e.message(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}
