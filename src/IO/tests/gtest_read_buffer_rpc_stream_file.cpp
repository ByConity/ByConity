#include <memory>
#include <fcntl.h>
#include <IO/ReadBufferFromRpcStreamFile.h>
#include <Storages/DistributedDataClient.h>
#include <Storages/DistributedDataService.h>
#include <Storages/tests/xml_config.h>
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
#include <Common/tests/gtest_utils.h>
#include "Core/Defines.h"

using namespace DB;

class ReadBufferFromRpcStreamFileTest : public ::testing::Test
{
public:
    static brpc::Server server;
    static DistributedDataService file_service;
    static void SetUpTestSuite()
    {
        try
        {
            UnitTest::initLogger();
            Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
            BrpcApplication::getInstance().initialize(*map_config);

            if (server.AddService(&file_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            {
                throw Exception("Fail to add remote cache service", DB::ErrorCodes::LOGICAL_ERROR);
            }

            // Start the server.
            brpc::ServerOptions options;
            options.idle_timeout_sec = -1;
            if (server.Start(12347, &options) != 0)
            {
                throw Exception("Fail to add remote cache server", DB::ErrorCodes::LOGICAL_ERROR);
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
brpc::Server ReadBufferFromRpcStreamFileTest::server;
DistributedDataService ReadBufferFromRpcStreamFileTest::file_service(73400320);

TEST_F(ReadBufferFromRpcStreamFileTest, base)
{
    try
    {
        sleep(3);
        auto client = std::make_shared<DistributedDataClient>("127.0.0.1:12347", TEST_DISTRIBUTED_FILE_TXT);
        auto stream_file = std::make_shared<ReadBufferFromRpcStreamFile>(client, DBMS_DEFAULT_BUFFER_SIZE);

        PODArray<char> buffer;
        buffer.resize(26);
        stream_file->seek(0, SEEK_SET);
        stream_file->readStrict(buffer.data(), 26);
        ASSERT_EQ(String(buffer.begin(), 26), "abcdefghijklmnopqrstuvwxyz");
    }
    catch (const Exception & e)
    {
        throw Exception(e.message(), DB::ErrorCodes::LOGICAL_ERROR);
    }
}
