#pragma once

#include <Common/Logger.h>
#include <array>
#include <atomic>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <ResourceManagement/CommonData.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <re2/re2.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>

namespace DB
{
struct StorageInfos
{
    std::vector<String> databases;
    std::vector<String> tables;
};

void tryGetStorageFromAST(const ASTPtr & ast, ContextMutablePtr & context, StorageInfos & storage_infos);

void enqueueVirtualWarehouseQueue(ContextMutablePtr context, ASTPtr & query_ast);

enum class VWQueueResultStatus : uint8_t
{
    QueueSuccess = 0,
    QueueFailed,
    QueueCancel,
    QueueOverSize,
    QueueStop,
    QueueTimeOut
};

const char * VWQueueResultStatusToString(VWQueueResultStatus status);
class VirtualWarehouseQueue;
struct DequeueRelease
{
    DequeueRelease() = default;
    ~DequeueRelease();
    void increase(VirtualWarehouseQueue * queue) { enqueue_counts[queue]++; }

    std::unordered_map<VirtualWarehouseQueue *, size_t> enqueue_counts;
};

using DequeueReleasePtr = std::shared_ptr<DequeueRelease>;
struct VWQueueInfo
{
    VWQueueInfo(const String & qid, ContextMutablePtr context_) : query_id(qid), context(context_) { enqueue_time = time(nullptr); }
    VWQueueInfo() = default;
    String query_id;
    std::atomic<VWQueueResultStatus> result{VWQueueResultStatus::QueueSuccess};
    bthread::ConditionVariable cv;
    ContextMutablePtr context;
    time_t enqueue_time{};
    Stopwatch sw;
    ResourceManagement::QueueRule query_rule;
};
using VWQueueInfoPtr = std::shared_ptr<VWQueueInfo>;
using VWQueryQueue = std::deque<VWQueueInfoPtr>;

struct QueueRuleWithRegex : public ResourceManagement::QueueRule
{
    QueueRuleWithRegex(const ResourceManagement::QueueRule & rule) : ResourceManagement::QueueRule(rule)
    {
        if (!rule.query_id.empty())
            query_id_regex = std::make_shared<RE2>(rule.query_id);
    }
    std::shared_ptr<RE2> query_id_regex;
};

using QueueRuleWithRegexVec = std::vector<QueueRuleWithRegex>;
class VirtualWarehouseQueue
{
public:
    VirtualWarehouseQueue() : log(getLogger("VirtualWarehouseQueue")) { }
    ~VirtualWarehouseQueue() { shutdown(); }
    void init(const std::string & queue_name_);
    void shutdown();
    void dequeue(size_t enqueue_time);
    VWQueueResultStatus enqueue(VWQueueInfoPtr queue_info, UInt64 timeout_ms);

    std::string queueName() const
    {
        std::shared_lock<std::shared_mutex> lock(rule_mutex);
        return queue_name;
    }
    size_t matchRules(const ResourceManagement::QueueRule & query_rule);

    void updateQueue(const ResourceManagement::QueueData & queue_data);

    size_t maxConcurrency() const
    {
        std::unique_lock lk(mutex);
        return max_concurrency;
    }

    size_t queryQueueSize() const
    {
        std::unique_lock lk(mutex);
        return query_queue_size;
    }

    QueueRuleWithRegexVec getRules() const
    {
        std::shared_lock<std::shared_mutex> lock(rule_mutex);
        return rules;
    }
    template <class F>
    void fillSystemTable(F && call)
    {
        call(*this);
    }

private:
    mutable bthread::Mutex mutex;
    bool is_stop{false};
    size_t query_queue_size{200};
    size_t max_concurrency{100};
    size_t current_parallelize_size{0};
    VWQueryQueue vw_query_queue;
    LoggerPtr log;

    //rules
    mutable std::shared_mutex rule_mutex;
    QueueRuleWithRegexVec rules;
    std::string queue_name;
};


class VirtualWarehouseQueueManager
{
public:
    void init();

    VirtualWarehouseQueueManager() : log(getLogger("VirtualWarehouseQueueManager")) { init(); }
    ~VirtualWarehouseQueueManager() { shutdown(); }
    void shutdown();
    void updateQueue(const std::vector<ResourceManagement::QueueData> & queue_datas);
    VWQueueResultStatus enqueue(VWQueueInfoPtr queue_info, UInt64 timeout_ms);

    template <class F>
    void fillSystemTable(F && call)
    {
        for (auto & queue : query_queues)
        {
            queue.fillSystemTable(call);
        }
    }

private:
    std::array<VirtualWarehouseQueue, static_cast<size_t>(QueueName::Count)> query_queues;
    std::atomic<bool> is_stop{false};
    LoggerPtr log;
};

} // namespace DB
