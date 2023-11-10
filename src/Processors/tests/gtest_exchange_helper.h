#pragma once

#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/Brpc/BrpcApplication.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include "Disks/DiskLocal.h"


class ExchangeRemoteTest : public ::testing::Test
{
protected:
    static brpc::Server server;
    static DB::BrpcExchangeReceiverRegistryService service_impl;
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
        if (server.Start(8001, &options) != 0)
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
        Coordination::BrpcApplication::getInstance().initialize(*map_config);
        startBrpcServer();

        auto context = getContext().context;
        DB::DiskExchangeDataManagerOptions options{.path = "bsp/v.1.0.0", .storage_policy = "default", .volume = "tmp/bsp"};
        DB::ContextWeakMutablePtr context_weak = std::weak_ptr<DB::Context>(context);
        fs::create_directories("tmp/bsp/");
        auto disk = std::make_shared<DB::DiskLocal>("bsp", "tmp/bsp/", 0);
        disk->createDirectories(options.path);
        context->setMockDiskExchangeDataManager(std::make_shared<DB::DiskExchangeDataManager>(context_weak, std::move(disk), options));
        ExchangeRemoteTest::service_impl.setContext(context);
        setQueryDuration();
    }

    static void TearDownTestCase()
    {
        server.Stop(1000);
    }

    void SetUp() override
    {
        auto context = getContext().context;
        manager = context->getDiskExchangeDataManager();
        disk = manager->getDisk();
        disk->removeRecursive("bsp/v.1.0.0");
        disk->createDirectories("bsp/v.1.0.0");
    }

    uint64_t query_unique_id = 111;
    String query_id = "query_id";
    uint64_t interval_ms = 10000;
    size_t rows = 7;
    UInt64 exchange_id = 1;
    UInt64 parallel_idx = 0;
    const String host = "localhost:6666";
    const String rpc_host = "127.0.0.1:8001";
    size_t write_segment_id = 0;
    size_t read_segment_id = 1;
    DB::DiskExchangeDataManagerPtr manager;
    DB::DiskPtr disk;
};
