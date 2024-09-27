#include <array>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/Brpc/BrpcApplication.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>

using namespace DB;
using namespace UnitTest;

class RPCchannelPoolTest : public ::testing::Test
{
protected:
    static brpc::Server server;
    static BrpcExchangeReceiverRegistryService service_impl;
    static void startBrpcServer()
    {
        if (server.AddService(&service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(ERROR) << "Fail to add service";
            return;
        }
        LOG(INFO) << "add service success";

        // Start the server.
        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        if (server.Start(8002, &options) != 0)
        {
            LOG(ERROR) << "Fail to start Server";
            return;
        }
        LOG(INFO) << "start Server";
    }

    static void SetUpTestCase()
    {
        UnitTest::initLogger();
        Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
        BrpcApplication::getInstance().initialize(*map_config);
        startBrpcServer();
    }

    static void TearDownTestCase()
    {
        server.Stop(1000);
    }
};

brpc::Server RPCchannelPoolTest::server;
BrpcExchangeReceiverRegistryService RPCchannelPoolTest::service_impl;

void get_client(
    size_t loop,
    const std::string & address,
    const std::string & client_type,
    bool check_pool_expire_timer,
    bool construct_random_exceptions)
{
    for (int i = 0; i < loop; i++)
    {
        auto client = RpcChannelPool::getInstance().getClient(address, client_type);
        if (construct_random_exceptions)
        {
            if (i % 10 == 0)
                client->setOk(false);
            if (i % 100 == 0)
                client->reportError();
        }
        if (check_pool_expire_timer)
        {
            if (i % 100 == 0)
                sleep(3);
        }
    }
}

TEST_F(RPCchannelPoolTest, single_address_concurrent)
{
    std::vector<std::thread> thread_get_clients;
    for (int i = 0; i < 100; i++)
    {
        std::thread thread_get_client(get_client, 10000, "127.0.0.1:8001", BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, false, false);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}

TEST_F(RPCchannelPoolTest, multi_address_concurrent)
{
    std::array<std::string, 2> client_types
        = {BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY};
    std::vector<std::thread> thread_get_clients;

    for (int i = 0; i < 100; i++)
    {
        auto address = "127.0.0.1:80" + std::to_string(i % 100);
        std::thread thread_get_client(get_client, 10000, address, client_types[i % 2], false, false);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}

TEST_F(RPCchannelPoolTest, check_pool_expire_timer_concurrent)
{
    RpcChannelPool::getInstance().initPoolExpireTimer(1, 1);

    std::array<std::string, 2> client_types
        = {BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY};
    std::vector<std::thread> thread_get_clients;

    for (int i = 0; i < 100; i++)
    {
        auto address = "127.0.0.1:80" + std::to_string(i % 100);
        std::thread thread_get_client(get_client, 10000, address, client_types[i % 2], false, false);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}

TEST_F(RPCchannelPoolTest, check_pool_expire_timer)
{
    RpcChannelPool::getInstance().initPoolExpireTimer(1, 1);

    std::array<std::string, 2> client_types
        = {BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY};
    std::vector<std::thread> thread_get_clients;

    for (int i = 0; i < 100; i++)
    {
        auto address = "127.0.0.1:80" + std::to_string(i % 100);
        std::thread thread_get_client(get_client, 10000, address, client_types[i % 2], true, false);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}

TEST_F(RPCchannelPoolTest, construct_random_exceptions)
{
    std::array<std::string, 2> client_types
        = {BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY};
    std::vector<std::thread> thread_get_clients;

    for (int i = 0; i < 100; i++)
    {
        auto address = "127.0.0.1:80" + std::to_string(i % 100);
        std::thread thread_get_client(get_client, 1000, address, client_types[i % 2], false, true);
        thread_get_clients.push_back(std::move(thread_get_client));
    }
    for (auto & th : thread_get_clients)
    {
        th.join();
    }
}
