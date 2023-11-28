#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
#include <mutex>
#include <string_view>

#include <Poco/Logger.h>

#include <common/types.h>

namespace DB::HybridCache
{
enum class JobExitCode
{
    Done,
    Reschedule,
};

using Job = std::function<JobExitCode()>;

enum class JobType
{
    Read,
    Write,
    Reclaim,
    Flush
};

class JobScheduler
{
public:
    virtual ~JobScheduler() = default;

    virtual void enqueueWithKey(Job job, std::string_view name, JobType type, UInt64 key) = 0;

    virtual void enqueue(Job job, std::string_view name, JobType type) = 0;

    virtual void finish() = 0;
};

std::unique_ptr<JobScheduler>
createOrderedThreadPoolJobScheduler(UInt32 reader_threads, UInt32 writer_threads, UInt32 req_order_shard_power);

class JobQueue
{
public:
    struct Stats
    {
        UInt64 jobs_done{};
        UInt64 jobs_high_reschedule{};
        UInt64 reschedules{};
        UInt64 max_queue_len{};
    };

    enum class QueuePos
    {
        Front,
        Back
    };

    JobQueue() = default;
    JobQueue(const JobQueue &) = delete;
    JobQueue & operator=(const JobQueue &) = delete;
    ~JobQueue() = default;

    void enqueue(Job job, std::string_view name, QueuePos pos);

    UInt64 finish();

    void process();

    void requestStop();

    Stats getStats() const;

private:
    Poco::Logger * log = &Poco::Logger::get("JobQueue");

    struct QueueEntry
    {
        Job job;
        UInt32 reschedule_count{};
        std::string_view name;

        QueueEntry(Job j, std::string_view n) : job{std::move(j)}, name{n} { }
        QueueEntry(QueueEntry &&) = default;
        QueueEntry & operator=(QueueEntry &&) = default;
    };

    JobExitCode runJob(QueueEntry & entry);

    mutable std::mutex mutex;
    mutable std::condition_variable cv;
    std::deque<QueueEntry> queue;
    UInt64 enqueue_count{};
    mutable UInt64 max_queue_len{};
    mutable UInt64 jobs_done{};
    mutable UInt64 jobs_high_reschedule{};
    mutable UInt64 reschedules{};
    int processing{};
    bool stop{false};
};

class ThreadPoolExecutor
{
public:
    struct Stats
    {
        UInt64 jobs_done{};
        UInt64 jobs_high_reschedule{};
        UInt64 reschedules{};
        UInt64 max_queue_len{};
        UInt64 max_pending_jobs{};
    };

    ThreadPoolExecutor(UInt32 num_threads, std::string_view name);

    void enqueue(Job job, std::string_view name, JobQueue::QueuePos pos);

    void enqueueWithKey(Job job, std::string_view name, JobQueue::QueuePos pos, UInt64 key);

    UInt64 finish();

    void join();

    Stats getStats() const;

    std::string_view getName() const { return name; }

private:
    Poco::Logger * log = &Poco::Logger::get("ThreadPoolExecutor");

    const std::string_view name{};
    std::atomic<UInt32> next_queue{0};
    std::vector<std::unique_ptr<JobQueue>> queues;
    std::vector<std::thread> workers;
};

class ThreadPoolJobScheduler final : public JobScheduler
{
public:
    explicit ThreadPoolJobScheduler(UInt32 reader_threads, UInt32 writer_threads);
    ThreadPoolJobScheduler(const ThreadPoolJobScheduler &) = delete;
    ThreadPoolJobScheduler & operator=(const ThreadPoolJobScheduler &) = delete;
    ~ThreadPoolJobScheduler() override { join(); }

    void enqueue(Job job, std::string_view name, JobType type) override;

    void enqueueWithKey(Job job, std::string_view name, JobType type, uint64_t key) override;

    void finish() override;


private:
    Poco::Logger * log = &Poco::Logger::get("ThreadPoolJobScheduler");

    void join();

    ThreadPoolExecutor reader;
    ThreadPoolExecutor writer;
};

class OrderedThreadPoolJobScheduler final : public JobScheduler
{
public:
    explicit OrderedThreadPoolJobScheduler(size_t reader_threads, size_t writer_threads, size_t num_shards_power);
    OrderedThreadPoolJobScheduler(const OrderedThreadPoolJobScheduler &) = delete;
    OrderedThreadPoolJobScheduler & operator=(const OrderedThreadPoolJobScheduler &) = delete;
    ~OrderedThreadPoolJobScheduler() override = default;

    void enqueue(Job job, std::string_view name, JobType type) override;

    void enqueueWithKey(Job job, std::string_view name, JobType type, UInt64 key) override;

    void finish() override;

private:
    struct JobParams
    {
        Job job;
        JobType type;
        std::string_view name;
        UInt64 key;

        JobParams(Job j, JobType t, std::string_view n, UInt64 k) : job{std::move(j)}, type{t}, name{n}, key{k} { }

        JobParams(const JobParams &) = delete;
        JobParams & operator=(const JobParams &) = delete;
        JobParams(JobParams && o) noexcept : job{std::move(o.job)}, type{o.type}, name{o.name}, key{o.key} { }
        JobParams & operator=(JobParams && o)
        {
            if (this != &o)
            {
                this->~JobParams();
                new (this) JobParams(std::move(o));
            }
            return *this;
        }
    };

    void scheduleNextJob(UInt64 shard);
    void scheduleJobLocked(JobParams params, UInt64 shard);

    mutable std::vector<std::mutex> mutexes;

    std::vector<std::list<JobParams>> pending_jobs;

    std::vector<UInt64> should_spool;

    const size_t num_shards_power;

    ThreadPoolJobScheduler scheduler;
};
}
