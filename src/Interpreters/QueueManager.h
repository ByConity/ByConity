#pragma once
#include <Common/Logger.h>
#include <atomic>
#include <deque>
#include <memory>
#include <unordered_map>
#include <vector>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTVisitor.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Common/Configurations.h>
#include <common/types.h>
#include <Common/Stopwatch.h>
namespace DB
{


inline String getWorkerGroupName(const String & vw_name, const String & wg_name)
{
    return vw_name + "." + wg_name;
}

enum class QueueResultStatus : uint8_t
{
    QueueSuccess = 0,
    QueueFailed,
    QueueCancel,
    QueueOverSize,
    QueueStop,
    QueueTimeOut
};

const char * queueResultStatusToString(QueueResultStatus status);

class QueueManager;
struct QueueInfo
{
    QueueInfo(const String & qid, const String & vw_name_, const String & wg_name_, ContextMutablePtr context_)
        : query_id(qid), vw_name(vw_name_), wg_name(wg_name_), context(context_)
    {
        vw = vw_name + "." + wg_name;
        enqueue_time = time(nullptr);
    }
    QueueInfo() = default;
    String query_id;
    String vw;
    String vw_name;
    String wg_name;
    std::atomic<QueueResultStatus> result{QueueResultStatus::QueueSuccess};
    bthread::ConditionVariable cv;
    ContextMutablePtr context;
    time_t enqueue_time{};
    Stopwatch sw;
};
using QueueInfoPtr = std::shared_ptr<QueueInfo>;
using QueryQueue = std::deque<QueueInfoPtr>;

class QueueManagerTriggerTask
{
public:
    QueueManagerTriggerTask(BackgroundSchedulePool & pool_, QueueManager * query_manager_, UInt64 interval_, const std::string & name_);

    void start() { task->activateAndSchedule(); }
    void stop() { task->deactivate(); }
    void setInterval(UInt64 _interval) { interval = _interval; }
    ~QueueManagerTriggerTask();
    void threadFunc();

private:
    std::atomic<UInt64> interval; /// in ms;
    BackgroundSchedulePool::TaskHolder task;
    QueueManager * queue_manager{nullptr};
};

class QeueueThrottler
{
public:
    virtual bool isThrottling(QueueInfo *) = 0;
    virtual ~QeueueThrottler() = default;
};

using QeueueThrottlerPtr = std::shared_ptr<QeueueThrottler>;
using QeueueThrottlersPtr = std::vector<QeueueThrottlerPtr>;

class VWConcurrencyQeueueThrottler;
using VWConcurrencyQeueueThrottlerPtr = std::shared_ptr<VWConcurrencyQeueueThrottler>;
struct QeueueThrottlerDeleter;
using QueueThrottlerDeleterPtr = std::shared_ptr<QeueueThrottlerDeleter>;

class VWConcurrencyQeueueThrottler : public QeueueThrottler
{
public:
    VWConcurrencyQeueueThrottler() = default;
    VWConcurrencyQeueueThrottler(QueueManager * queue_manager_) : VWConcurrencyQeueueThrottler() { queue_manager = queue_manager_; }
    virtual bool isThrottling(QueueInfo * queue_info) override;
    QueueThrottlerDeleterPtr getDeleter(const String & vw);
    void release(const String & vw);
    virtual ~VWConcurrencyQeueueThrottler() override = default;

    std::unordered_map<String, size_t> vw_parallel_map;
    bthread::Mutex mutex;
    QueueManager * queue_manager;
};

struct QeueueThrottlerDeleter
{
    QeueueThrottlerDeleter(VWConcurrencyQeueueThrottler * ptr_, const String & vw_) : ptr(ptr_), vw(vw_) { }
    ~QeueueThrottlerDeleter() { ptr->release(vw); }
    VWConcurrencyQeueueThrottler * ptr;
    const String vw;
};

class QueueManager : public WithContext
{
public:
    static inline const String prefix{"queue_manager"};
    QueueManager(ContextWeakMutablePtr context_);
    ~QueueManager();
    QueueResultStatus enqueue(QueueInfoPtr queue_info_ptr, UInt64 timeout_ms);
    void cancel(const String & query_id);
    void trigger();
    void loadConfig(const QMConfiguration & config);
    void triggerVW(QueryQueue & vw_queue, QueueResultStatus status, size_t batch_size);
    bool isStop() const { return is_stop; }
    size_t getVWParallelizeSize() { return vw_parallelize_size; }
    void stop() { is_stop = true; }
    void shutdown();

    template <class Callback>
    void getQueueInfo(Callback && callback)
    {
        std::lock_guard lg(mutex);
        for (const auto & [_, queue] : query_queues)
        {
            for (const auto & queue_info_ptr : queue)
            {
                callback(queue_info_ptr);
            }
        }
    }

private:
    bthread::Mutex mutex;
    size_t query_queue_size{100};
    std::atomic<size_t> vw_parallelize_size{20};
    std::atomic<size_t> batch_size{2};
    std::atomic<bool> is_stop{false};

    //vw -> query queue
    std::unordered_map<String, QueryQueue> query_queues;

    mutable std::optional<BackgroundSchedulePool> schedule_pool;
    std::unique_ptr<QueueManagerTriggerTask> queue_manager_trigger_task;
    QeueueThrottlersPtr throttlers;
    LoggerPtr log;
    VWConcurrencyQeueueThrottlerPtr vw_concurrency_throttler;
};

using QueueManagerPtr = std::shared_ptr<QueueManager>;
}
