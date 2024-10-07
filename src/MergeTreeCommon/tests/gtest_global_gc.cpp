#include <MergeTreeCommon/GlobalGCManager.h>
#include <Common/tests/gtest_global_context.h>
#include <iostream>
#include <string>
#include <list>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/AutoPtr.h>
#include <Protos/data_models.pb.h>
#include <Protos/RPCHelpers.h>
#include <Core/UUID.h>
#include <gtest/gtest.h>
#include <chrono>

using namespace DB;
using namespace DB::GlobalGCHelpers;

namespace GTEST_GLOBAL_GC
{

std::mutex mutex1; // to hang executeGlobalGCDummy1
bool executeGlobalGCDummy1(const Protos::DataModelTable & /*table*/, const Context &, LoggerPtr)
{
    std::lock_guard<std::mutex> lock(mutex1);
    //std::cout << "executed table:" << UUIDHelpers::UUIDToString(RPCHelpers::createUUID(table.uuid())) << "\n";
    return true;
}

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

ConfigurationPtr getConfiguration()
{
    std::string xml_data = R"#(<?xml version="1.0"?>
<yandex>
</yandex>
)#";
    std::stringstream ss(xml_data);
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}

std::vector<Protos::DataModelTable> createTables(int begin, int end)
{
    int size = end - begin;
    std::vector<Protos::DataModelTable> res(size);
    for (size_t i = 0; i < res.size(); ++i)
    {
        RPCHelpers::fillUUID(UUID{UInt128{0ul, begin + i}}, *res[i].mutable_uuid());
    }

    return res;
}

std::set<UUID> createUUIDs(int begin, int end)
{
    int size = end - begin;
    std::set<UUID> res;
    for (size_t i = 0; i < size; ++i)
    {
        res.insert(UUID{UInt128{0ul, begin + i}});
    }

    return res;
}

TEST(GlobalGCManager, calculateApproximateWorkLimit_test)
{
    {
        size_t max_threads = 3;
        size_t res = calculateApproximateWorkLimit(max_threads);
        EXPECT_EQ(res, 60);
    }
    {
        size_t max_threads = 0;
        size_t res = calculateApproximateWorkLimit(max_threads);
        EXPECT_EQ(res, 0);
    }
}

TEST(GlobalGCManager, amountOfWorkCanReceive_test)
{
    {
        size_t max_threads = 3;
        size_t deleting_table_num = 69;
        size_t ret = amountOfWorkCanReceive(max_threads, deleting_table_num);
        EXPECT_EQ(ret, 0);
    }

    {
        size_t max_threads = 3;
        size_t deleting_table_num = 60;
        size_t ret = amountOfWorkCanReceive(max_threads, deleting_table_num);
        EXPECT_EQ(ret, 0);
    }

    {
        size_t max_threads = 3;
        size_t deleting_table_num = 59;
        size_t ret = amountOfWorkCanReceive(max_threads, deleting_table_num);
        EXPECT_EQ(ret, 10);
    }
    {
        size_t max_threads = 3;
        size_t deleting_table_num = 49;
        size_t ret = amountOfWorkCanReceive(max_threads, deleting_table_num);
        EXPECT_EQ(ret, 20);
    }
    {
        size_t max_threads = 3;
        size_t deleting_table_num = 9;
        size_t ret = amountOfWorkCanReceive(max_threads, deleting_table_num);
        EXPECT_EQ(ret, 60);
    }

    {
        size_t max_threads = 3;
        size_t deleting_table_num = 0;
        size_t ret = amountOfWorkCanReceive(max_threads, deleting_table_num);
        EXPECT_EQ(ret, 60);
    }

    {
        size_t max_threads = 0;
        size_t deleting_table_num = 0;
        size_t ret = amountOfWorkCanReceive(max_threads, deleting_table_num);
        EXPECT_EQ(ret, 0);
    }
}

TEST(GlobalGCManager, canReceiveMoreWork_test)
{

    {
        size_t max_threads = 0;
        size_t deleting_table_num = 0;
        size_t number_of_new_table = 1;
        bool ret = canReceiveMoreWork(max_threads, deleting_table_num, number_of_new_table);
        EXPECT_EQ(ret, false);
    }

    {
        size_t max_threads = 3;
        size_t deleting_table_num = 1;
        size_t number_of_new_table = 10;
        bool ret = canReceiveMoreWork(max_threads, deleting_table_num, number_of_new_table);
        EXPECT_EQ(ret, true);

        ret = canReceiveMoreWork(max_threads, deleting_table_num, 68);
        EXPECT_EQ(ret, true);

        ret = canReceiveMoreWork(max_threads, deleting_table_num, 69);
        EXPECT_EQ(ret, false);
    }
    {
        size_t max_threads = 3;
        size_t deleting_table_num = 60;
        size_t number_of_new_table = 1;
        bool ret = canReceiveMoreWork(max_threads, deleting_table_num, number_of_new_table);
        EXPECT_EQ(ret, false);
    }
}

TEST(GlobalGCManager, getUUIDsFromTables_test)
{
    std::vector<Protos::DataModelTable> tables = createTables(1, 6);
    std::vector<UUID> uuids = GlobalGCHelpers::getUUIDsFromTables(tables);
    std::set<UUID> expected_uuids_set = createUUIDs(1, 6);
    std::vector<UUID> expected_uuids(expected_uuids_set.begin(), expected_uuids_set.end());
    EXPECT_EQ(uuids, expected_uuids);
}

TEST(GlobalGCManager, removeDuplication_test0)
{
    std::set<UUID> deleting_uuids;
    std::vector<Protos::DataModelTable> tables = createTables(1, 6);
    tables = GlobalGCHelpers::removeDuplication(deleting_uuids, std::move(tables));
    std::vector<Protos::DataModelTable> expected_tables = createTables(1, 6);
    EXPECT_EQ(GlobalGCHelpers::getUUIDsFromTables(tables), GlobalGCHelpers::getUUIDsFromTables(expected_tables));
}

TEST(GlobalGCManager, removeDuplication_test1)
{
    {
        std::set<UUID> deleting_uuids;
        std::vector<Protos::DataModelTable> tables = createTables(1, 6);
        std::vector<Protos::DataModelTable> tables_clone = tables;
        std::copy(tables_clone.begin(), tables_clone.end(), std::back_inserter(tables));
        tables = removeDuplication(deleting_uuids, std::move(tables));
        EXPECT_EQ(getUUIDsFromTables(tables), getUUIDsFromTables(tables_clone));
    }

    {
        std::set<UUID> deleting_uuids = createUUIDs(1, 3);
        std::vector<Protos::DataModelTable> tables = createTables(1, 6);
        std::vector<Protos::DataModelTable> tables_clone = tables;
        std::copy(tables_clone.begin(), tables_clone.end(), std::back_inserter(tables));
        tables = removeDuplication(deleting_uuids, std::move(tables));
        std::vector<UUID> uuids_vec = getUUIDsFromTables(tables);
        std::set<UUID> result_uuids(uuids_vec.begin(), uuids_vec.end());
        std::set<UUID> expected_uuids = createUUIDs(3, 6);
        EXPECT_EQ(result_uuids, expected_uuids);
    }
}

TEST(GlobalGCManager, disable_threadpool_test)
{
    auto config = getConfiguration();
    DB::ContextMutablePtr g_context = getContext().context;
    ContextMutablePtr context = Context::createCopy(g_context);
    context->setConfig(config);
    GlobalGCManager global_gc(context, 0, 0, 0);
    std::vector<Protos::DataModelTable> tables = createTables(1, 6);
    EXPECT_EQ(global_gc.getMaxThreads(), 0);
    EXPECT_EQ(global_gc.schedule(tables), false);
}


TEST(GlobalGCManager, normal_operation_test0)
{
    auto config = getConfiguration();
    DB::ContextMutablePtr g_context = getContext().context;
    ContextMutablePtr context = Context::createCopy(g_context);
    context->setConfig(config);

    GlobalGCManager global_gc(context, 1, 1, 3);
    global_gc.setExecutor(executeGlobalGCDummy1);
    ThreadPool * threadpool = global_gc.getThreadPool();
    EXPECT_NE(threadpool, nullptr);
    EXPECT_EQ(threadpool->active(), 0);

    EXPECT_EQ(global_gc.getMaxThreads(), 1);
    EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 0);
    EXPECT_EQ(global_gc.getDeletingUUIDs().empty(), true);
    EXPECT_EQ(global_gc.isShutdown(), false);

    {
        std::lock_guard<std::mutex> lock(mutex1); //hang the executor
        {
            std::vector<Protos::DataModelTable> tables = createTables(1, 6);
            EXPECT_EQ(global_gc.schedule(tables), true);
            std::set<UUID> uuids = global_gc.getDeletingUUIDs();
            std::set<UUID> expected_uuid = createUUIDs(1,6);
            EXPECT_EQ(uuids, expected_uuid);
            EXPECT_EQ(threadpool->active(), 1);
            EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 5);
        }
        {
            std::vector<Protos::DataModelTable> tables = createTables(6, 11);
            EXPECT_EQ(global_gc.schedule(tables), true);
            std::set<UUID> uuids = global_gc.getDeletingUUIDs();
            std::set<UUID> expected_uuid = createUUIDs(1,11);
            EXPECT_EQ(uuids, expected_uuid);
            EXPECT_EQ(threadpool->active(), 2);
            EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 10);
        }
    }
    // unhang the executor, and wait for job to be done
    threadpool->wait();
    EXPECT_EQ(global_gc.getDeletingUUIDs().empty(), true);
    global_gc.shutdown();
    EXPECT_EQ(global_gc.isShutdown(), true);

    {
        std::vector<Protos::DataModelTable> tables = createTables(1, 6);
        EXPECT_EQ(global_gc.schedule(tables), false);
    }
}

TEST(GlobalGCManager, normal_operation_test_split_work)
{
    auto config = getConfiguration();
    DB::ContextMutablePtr g_context = getContext().context;
    ContextMutablePtr context = Context::createCopy(g_context);
    context->setConfig(config);

    GlobalGCManager global_gc(context, 1, 1, 10);
    global_gc.setExecutor(executeGlobalGCDummy1);
    ThreadPool * threadpool = global_gc.getThreadPool();

    {
        std::lock_guard<std::mutex> lock(mutex1); //hang the executor
        {
            std::vector<Protos::DataModelTable> tables = createTables(1, 2);
            EXPECT_EQ(global_gc.schedule(tables), true);
            std::set<UUID> uuids = global_gc.getDeletingUUIDs();
            std::set<UUID> expected_uuid = createUUIDs(1, 2);
            EXPECT_EQ(uuids, expected_uuid);
            EXPECT_EQ(threadpool->active(), 1);
            EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 1);
        }
        {
            /// resend the work, not duplicate code
            std::vector<Protos::DataModelTable> tables = createTables(1, 2);
            EXPECT_EQ(global_gc.schedule(tables), true);
            std::set<UUID> uuids = global_gc.getDeletingUUIDs();
            std::set<UUID> expected_uuid = createUUIDs(1, 2);
            EXPECT_EQ(uuids, expected_uuid);
            EXPECT_EQ(threadpool->active(), 1);
            EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 1);
        }
        {
            std::vector<Protos::DataModelTable> tables = createTables(2, 22);
            EXPECT_EQ(global_gc.schedule(tables), true);
            std::set<UUID> uuids = global_gc.getDeletingUUIDs();
            std::set<UUID> expected_uuid = createUUIDs(1,22);
            EXPECT_EQ(uuids, expected_uuid);
            EXPECT_EQ(threadpool->active(), 3);
            EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 21);
        }
        {
            std::vector<Protos::DataModelTable> tables = createTables(22, 23);
            EXPECT_EQ(global_gc.schedule(tables), false);
            std::set<UUID> uuids = global_gc.getDeletingUUIDs();
            std::set<UUID> expected_uuid = createUUIDs(1, 22);
            EXPECT_EQ(uuids, expected_uuid);
            EXPECT_EQ(threadpool->active(), 3);
            EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 21);
        }
    }
    // unhang the executor, and wait for job to be done
    threadpool->wait();
    EXPECT_EQ(global_gc.getDeletingUUIDs().empty(), true);
    global_gc.shutdown();
}

TEST(GlobalGCManager, normal_operation_test_full_queue)
{
    auto config = getConfiguration();
    DB::ContextMutablePtr g_context = getContext().context;
    ContextMutablePtr context = Context::createCopy(g_context);
    context->setConfig(config);

    GlobalGCManager global_gc(context, 3, 1, 2);
    global_gc.setExecutor(executeGlobalGCDummy1);
    ThreadPool * threadpool = global_gc.getThreadPool();

    {
        std::lock_guard<std::mutex> lock(mutex1); //hang the executor
        {
            std::vector<Protos::DataModelTable> tables = createTables(1, 61);
            EXPECT_EQ(global_gc.schedule(tables), false);
            std::set<UUID> uuids = global_gc.getDeletingUUIDs();
            std::set<UUID> expected_uuid = createUUIDs(1,21);
            EXPECT_EQ(uuids, expected_uuid);
            EXPECT_EQ(threadpool->active(), 2);
            EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 20);
        }
    }
    // unhang the executor, and wait for job to be done
    threadpool->wait();
    EXPECT_EQ(global_gc.getDeletingUUIDs().empty(), true);
    global_gc.shutdown();
}

bool executeGlobalGCDummy2(const Protos::DataModelTable & /*table*/, const Context &, LoggerPtr)
{
    sleep(1);
    return true;
}

TEST(GlobalGCManager, graceful_shutdown_test)
{
    auto config = getConfiguration();
    DB::ContextMutablePtr g_context = getContext().context;
    ContextMutablePtr context = Context::createCopy(g_context);
    context->setConfig(config);

    GlobalGCManager global_gc(context, 3, 1, 6);
    EXPECT_EQ(global_gc.getMaxThreads(), 3);
    global_gc.setExecutor(executeGlobalGCDummy1);
    ThreadPool * threadpool = global_gc.getThreadPool();
    {
        std::lock_guard<std::mutex> lock(mutex1); //hang the executor
        std::vector<Protos::DataModelTable> tables1 = createTables(1, 6);
        EXPECT_EQ(global_gc.schedule(tables1), true);

        std::vector<Protos::DataModelTable> tables2 = createTables(6, 11);
        EXPECT_EQ(global_gc.schedule(tables2), true);

        std::vector<Protos::DataModelTable> tables3 = createTables(11, 16);
        EXPECT_EQ(global_gc.schedule(tables3), true);

        std::set<UUID> uuids = global_gc.getDeletingUUIDs();
        std::set<UUID> expected_uuid = createUUIDs(1,16);
        EXPECT_EQ(uuids, expected_uuid);
        EXPECT_EQ(threadpool->active(), 3);
        EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 15);
        std::this_thread::sleep_for (std::chrono::seconds(1));
        global_gc.shutdown();
        EXPECT_EQ(global_gc.isShutdown(), true);
    }
    threadpool->wait();
    EXPECT_EQ(threadpool->active(), 0);
    EXPECT_EQ(global_gc.getNumberOfDeletingTables(), 12);
    {
        std::set<UUID> uuids = global_gc.getDeletingUUIDs();
        std::set<UUID> batch1_remain_uuids = createUUIDs(2, 6);
        std::set<UUID> batch2_remain_uuids = createUUIDs(7, 11);
        std::set<UUID> batch3_remain_uuids = createUUIDs(12, 16);
        std::set<UUID> expected_uuids;
        std::copy(batch1_remain_uuids.begin(), batch1_remain_uuids.end(), std::inserter(expected_uuids, expected_uuids.end()));
        std::copy(batch2_remain_uuids.begin(), batch2_remain_uuids.end(), std::inserter(expected_uuids, expected_uuids.end()));
        std::copy(batch3_remain_uuids.begin(), batch3_remain_uuids.end(), std::inserter(expected_uuids, expected_uuids.end()));
        EXPECT_EQ(uuids, expected_uuids);
    }
}

}
