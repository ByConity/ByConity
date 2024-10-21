#pragma once

#include <Disks/DiskLocal.h>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <Poco/Util/MapConfiguration.h>
#include <common/types.h>
#include <Common/Brpc/BrpcApplication.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>


const int brpc_server_port = 8001;

struct MockServiceDiscoveryClient : public DB::IServiceDiscovery
{
    DB::HostWithPortsVec lookup(const String & psm_name, DB::ComponentType type, const String & vw_name = "", UInt32 ttl = 0) override;
    std::string getName() const override
    {
        return "MockServiceDiscoveryClient";
    }
    DB::ServiceDiscoveryMode getType() const override
    {
        return DB::ServiceDiscoveryMode::LOCAL;
    }
};

class ExchangeRemoteTest : public ::testing::Test
{
protected:
    static brpc::Server * server;
    static DB::BrpcExchangeReceiverRegistryService * service_impl;
    static void startBrpcServer()
    {
        if (server->AddService(service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(ERROR) << "Fail to add service";
            return;
        }
        LOG(INFO) << "add service success";

        // Start the server.
        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        if (server->Start(brpc_server_port, &options) != 0)
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
        Coordination::SettingsChanges config_settings;
        config_settings.emplace_back("exchange_timeout_ms", 20000);
        config_settings.emplace_back("temporary_files_codec", "NONE");
        context->applySettingsChanges(config_settings);
        /// gc_interval_seconds is set to zero for testing
        DB::ContextWeakMutablePtr context_weak = std::weak_ptr<DB::Context>(context);
        fs::create_directories("tmp_bsp/");
        auto disk = std::make_shared<DB::DiskLocal>("bsp", "tmp_bsp/", DB::DiskStats{});
        DB::DiskExchangeDataManagerOptions options{
            .path = "bsp/v-1.0.0",
            .storage_policy = "default",
            .volume = "tmp_bsp",
            .gc_interval_seconds = 1000,
            .file_expire_seconds = 10000,
            .max_disk_bytes = 100000,
            .start_gc_random_wait_seconds = 0};
        if (disk->exists(options.path))
            disk->removeRecursive(options.path);
        disk->createDirectories(options.path);
        auto mock_sd_client = std::make_shared<MockServiceDiscoveryClient>();
        std::shared_ptr<DB::IServiceDiscovery> sd_client = mock_sd_client;
        context->setMockDiskExchangeDataManager(
            std::make_shared<DB::DiskExchangeDataManager>(context_weak, std::move(disk), options, sd_client, ""));
        context->setMockExchangeDataTracker(std::make_shared<DB::ExchangeStatusTracker>(context_weak));
        ExchangeRemoteTest::service_impl->setContext(context);
        setQueryDuration();
    }

    static void TearDownTestCase()
    {
        getContext().context->getDiskExchangeDataManager()->shutdown();
        server->Stop(1000);
    }

    void SetUp() override
    {
        auto context = getContext().context;
        DB::ContextWeakMutablePtr context_weak = std::weak_ptr<DB::Context>(context);
        manager = context->getDiskExchangeDataManager();
        manager->setFileExpireSeconds(10000);
        disk = manager->getDisk();
    }

    uint64_t query_unique_id_1 = 111;
    uint64_t query_unique_id_2 = 222;
    uint64_t query_unique_id_3 = 333;
    uint64_t query_unique_id_4 = 444;
    String query_id = "query_id";
    uint64_t interval_ms = 10000;
    size_t rows = 7;
    UInt64 exchange_id = 1;
    UInt32 parallel_idx = 0;
    const String host = "localhost:6666";
    const String rpc_host = "127.0.0.1:8001";
    size_t write_segment_id = 0;
    size_t read_segment_id = 1;
    DB::DiskExchangeDataManagerPtr manager;
    DB::DiskPtr disk;
};
