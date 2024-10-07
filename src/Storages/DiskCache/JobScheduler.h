#pragma once

#include <Common/Logger.h>
#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
#include <mutex>
#include <string_view>

#include <Poco/Logger.h>

#include <common/types.h>

#include <Storages/DiskCache/FiberThread.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/io/async/EventBase.h>

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

    // Notify the completion of the job (only for NavyRequestScheduler)
    virtual void notifyCompletion(uint64_t) { throw std::runtime_error("notifyCompletion is not supported"); }
};

std::unique_ptr<JobScheduler>
createOrderedThreadPoolJobScheduler(UInt32 reader_threads, UInt32 writer_threads, UInt32 req_order_shard_power);

std::unique_ptr<JobScheduler> createFiberRequestScheduler(
    size_t reader_threads,
    size_t writer_threads,
    size_t max_concurrent_reads,
    size_t max_concurrent_writes,
    size_t stack_size,
    size_t num_shards_power);
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
    LoggerPtr log = getLogger("JobQueue");

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
    LoggerPtr log = getLogger("ThreadPoolExecutor");

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
    LoggerPtr log = getLogger("ThreadPoolJobScheduler");

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


// This class represents the parameters describing a job to be scheduled
class FiberRequest
{
public:
    explicit FiberRequest(Job && job_, std::string_view name_, JobType type_, uint64_t key_)
        : job(std::move(job_)), name(name_), type(type_), key(key_)
    {
    }
    // beginTime_(util::getCurrentTimeNs()) {}

    virtual ~FiberRequest() = default;

    // Return the type of the job
    JobType getType() const { return type; }

    // Return the key of the job
    uint64_t getKey() const { return key; }

    // Main function to run the request
    JobExitCode execute() { return job(); }

    // next_ is a hook used to implement the atomic singly linked list
    FiberRequest * next = nullptr;

protected:
    Job job;

    const std::string_view name;

    const JobType type;

    // Key of the Request
    const uint64_t key;
};

// NavyRequestDispatcher is a request dispatcher with MPSC atomic submission
// queue. For efficiency, this class implements a simple MPSC queue using an
// atomic intrusive singly linked list. Specifically, a new request is added
// at the head of the list in an atomic manner, so that list maintains
// requests in LIFO order. The actual dispatch is performed by a fiber task,
// which is started when the first request is pushed to the queue. In order to
// guarantee that a single dispatcher task is running, the dispatcher task
// claims the submission queue by replacing the head of the list with the
// sentinel value while dispatching. For actual dispatch, the dispatcher task
// needs to reverse the linked list to submit in FIFO order.
class FiberRequestDispatcher
{
public:
    // @param scheduler       the parent scheduler to get completion notification
    // @param name            name of the dispatcher
    // @param maxOutstanding  maximum number of concurrently running requests
    FiberRequestDispatcher(JobScheduler & scheduler, std::string_view name, size_t max_outstanding, size_t stack_size);

    std::string_view getName() { return name; }

    // Add a new request to the dispatch queue
    void submitReq(std::unique_ptr<FiberRequest> req);

    // Wrapper to add task to the worker thread of this dispatcher
    void addTaskRemote(folly::Func func) { worker.addTaskRemote(std::move(func)); }

    UInt64 getNumSubmitted() { return num_submitted.load(std::memory_order_seq_cst); }
    UInt64 getNumCompleted() { return num_completed.load(std::memory_order_seq_cst); }
    UInt64 getNumOutstanding() { return cur_outstanding.load(std::memory_order_seq_cst); }

    // Wait until all jobs finished, and return totol number of submitted jobs
    UInt64 finish();

private:
    // Request dispatch loop
    void processLoop();

    // Actually submit the req to the worker thread
    void scheduleReq(std::unique_ptr<FiberRequest> req);

    LoggerPtr log = getLogger("NavyRequestDispatcher");

    // The parent scheduler to get completion notification
    JobScheduler & scheduler;
    // Name of the dispatcher
    std::string name;
    // The head of the custom implementation of atomic MPSC queue
    FiberRequest * incoming_reqs{nullptr};
    // Maximum number of outstanding requests
    size_t max_outstanding;
    // Baton used for waiting when limited by maxOutstanding_
    folly::fibers::Baton baton;
    // Worker thread
    FiberThread worker;

    // Internal atomic counters for stats tracking
    std::atomic<UInt64> cur_outstanding{0};
    std::atomic<UInt64> num_submitted{0};
    std::atomic<UInt64> num_completed{0};
    // std::atomic<UInt64> numPolled_{0};
    // std::atomic<UInt64> numDispatched_{0};
};

// NavyRequestScheduler is a NavyRequest dispatcher with resolving any
// data dependencies. For this, NavyRequestScheduler performs spooling.
// For actual worker, two types of NavyRequestDispatcher are instantiated,
// one for read and the other for the rest of request types
class FiberRequestScheduler : public JobScheduler
{
public:
    // @param readerThreads   number of threads for the read scheduler
    // @param writerThreads   number of threads for the write scheduler
    // @param numShardsPower  power of two specification for sharding internally
    //                        to avoid contention and queueing
    explicit FiberRequestScheduler(
        size_t reader_threads,
        size_t writer_threads,
        size_t max_concurrent_reads,
        size_t max_concurrent_writes,
        size_t stack_size,
        size_t num_shards_power);
    FiberRequestScheduler(const FiberRequestScheduler &) = delete;
    FiberRequestScheduler & operator=(const FiberRequestScheduler &) = delete;
    ~FiberRequestScheduler() override;

    void enqueue(Job job, std::string_view name, JobType type) override;

    // Put a job into the queue based on the key hash
    // execution ordering of the key is guaranteed
    void enqueueWithKey(Job job, std::string_view name, JobType type, uint64_t key) override;

    // Notify the completion of the request
    void notifyCompletion(uint64_t key) override;

    // waits till all queued and currently running jobs are finished
    void finish() override;

private:
    // Return the context for the key and type
    FiberRequestDispatcher & getDispatcher(uint64_t keyHash, JobType type);

    LoggerPtr log = getLogger("NavyRequestScheduler");

    const size_t num_reader_threads;
    const size_t num_writer_threads;
    // sharding power
    const size_t num_shards;

    bool stopped{false};

    std::vector<std::shared_ptr<FiberRequestDispatcher>> reader_dispatchers;
    std::vector<std::shared_ptr<FiberRequestDispatcher>> writer_dispatchers;

    // mutex per shard. mutex protects the pending jobs and spooling state
    mutable std::vector<folly::fibers::TimedMutex> mutexes;

    // list of jobs per shard if there is already a job pending for the shard
    std::vector<std::list<std::unique_ptr<FiberRequest>>> pending_reqs;

    // indicates if there is a pending job for the shard. This is kept as an
    // uint64_t instead of bool to ensure atomicity with sharded locks.
    std::vector<UInt64> should_spool;

    // use for enqueue without a key
    std::atomic<UInt32> next_key{0};

    std::atomic<UInt64> num_enqueued{0};
};

}
