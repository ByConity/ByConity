/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Common/Logger.h>
#include <set>
#include <mutex>
#include <Common/ThreadPool.h>
#include <Core/UUID.h>
#include <Interpreters/StorageID.h>
#include <Protos/data_models.pb.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{

namespace GlobalGCHelpers
{
constexpr size_t DEFAULT_THREADPOOL_MAX_SIZE = 3;
constexpr size_t DEFAULT_THREADPOOL_MAX_FREE_THREAD = 1;
constexpr size_t DEFAULT_THREADPOOL_MAX_QUEUE_SIZE = 60;

/**
 * @return if the table can be cleaned, returns the latest version of metadata.
 *         otherwise returns nullopt and set `fail_reason` if it's not nullptr.
 */
std::optional<Protos::DataModelTable> getCleanableTrashTable(
    ContextPtr context,
    const Protos::TableIdentifier & table_id,
    const TxnTimestamp & ts,
    UInt64 retention_sec,
    String * fail_reason = nullptr);

using GlobalGCExecuter = std::function<bool(const Protos::DataModelTable & table, const Context & context, LoggerPtr log)>;
bool executeGlobalGC(const Protos::DataModelTable & table, const Context & context, LoggerPtr log);

size_t calculateApproximateWorkLimit(size_t max_threads);
bool canReceiveMoreWork(size_t max_threads, size_t deleting_table_num, size_t num_of_new_tables);
size_t amountOfWorkCanReceive(size_t max_threads, size_t deleting_table_num);
std::vector<Protos::DataModelTable> removeDuplication(
    const std::set<UUID> & deleting_uuids,
    std::vector<Protos::DataModelTable> tables
);
std::vector<UUID> getUUIDsFromTables(const std::vector<Protos::DataModelTable> & table);
} /// end namespace GlobalGCHelpers


/// variant: never hold the mutex while call another function
class GlobalGCManager : public WithContext
{
public:
    struct GlobalGCTask
    {
        GlobalGCTask(std::vector<Protos::DataModelTable> tables, GlobalGCManager & manager);
        void operator()();
        std::vector<Protos::DataModelTable> tables;
        GlobalGCManager & manager;
    };

    static constexpr size_t MAX_BATCH_WORK_SIZE = 10;
    GlobalGCManager(
        ContextMutablePtr global_context_,
        size_t default_max_threads = GlobalGCHelpers::DEFAULT_THREADPOOL_MAX_SIZE,
        size_t default_max_free_threads = GlobalGCHelpers::DEFAULT_THREADPOOL_MAX_FREE_THREAD,
        size_t default_max_queue_size = GlobalGCHelpers::DEFAULT_THREADPOOL_MAX_QUEUE_SIZE);

    GlobalGCManager(const GlobalGCManager & other) = delete;
    GlobalGCManager & operator = (const GlobalGCManager & other) = delete;
    GlobalGCManager(GlobalGCManager && other) = delete;
    GlobalGCManager & operator = (GlobalGCManager && other) = delete;
    bool schedule(std::vector<Protos::DataModelTable> tables);
    void setExecutor(GlobalGCHelpers::GlobalGCExecuter executor_) { executor = executor_; }
    void shutdown();
    ~GlobalGCManager();
    /// for testing
    ThreadPool * getThreadPool() { return threadpool.get(); }

    size_t getNumberOfDeletingTables() const;
    size_t getMaxThreads() const { return max_threads; }
    std::set<UUID> getDeletingUUIDs() const;
    bool isShutdown() const;

    static void systemCleanTrash(ContextPtr local_context, StorageID storage_id, LoggerPtr log);

private:
    bool scheduleImpl(std::vector<Protos::DataModelTable> && tables);
    void removeDeletingUUID(UUID uuid);

    mutable std::mutex mutex;
    size_t max_threads;
    std::set<UUID> deleting_uuids;
    std::unique_ptr<ThreadPool> threadpool;
    bool is_shutdown = false;
    LoggerPtr log;
    GlobalGCHelpers::GlobalGCExecuter executor = GlobalGCHelpers::executeGlobalGC;
};

} /// end namespace
