#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>

#include <Storages/DiskCache/JobScheduler.h>
#include <fmt/core.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace ErrorCodes
{
const extern int BAD_ARGUMENTS;
}

namespace DB::HybridCache
{
namespace
{
    constexpr UInt64 kHighRescheduleCount = 250;
    constexpr UInt64 kHighRescheduleReportRate = 100;

    template <typename T>
    class ScopedUnlock
    {
    public:
        explicit ScopedUnlock(std::unique_lock<T> & lock_) : lock{lock_} { lock.unlock(); }

        ScopedUnlock(const ScopedUnlock &) = delete;
        ScopedUnlock & operator=(const ScopedUnlock &) = delete;
        ~ScopedUnlock() { lock.lock(); }

    private:
        std::unique_lock<T> & lock;
    };

}

template <typename Value, typename P, typename F>
bool atomicUpdateValue(Value * ref_ptr, Value * old_value, P && predicate, F && new_value_f)
{
    unsigned int n_CAS_Failures = 0;
    constexpr bool is_weak = false;
    Value cur_value = __atomic_load_n(ref_ptr, __ATOMIC_RELAXED);
    while (true)
    {
        if (!predicate(cur_value))
        {
            return false;
        }

        const Value new_value = new_value_f(cur_value);
        if (__atomic_compare_exchange_n(ref_ptr, &cur_value, new_value, is_weak, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
        {
            if (old_value)
            {
                *old_value = cur_value;
            }
            return true;
        }

        if ((++n_CAS_Failures % 4) == 0)
        {
            // this pause takes up to 40 clock cycles on intel and the lock cmpxchgl
            // above should take about 100 clock cycles. we pause once every 400
            // cycles or so if we are extremely unlucky.
            folly::asm_volatile_pause();
        }
    }
}

void JobQueue::enqueue(Job job, std::string_view name, QueuePos pos)
{
    bool was_empty = false;
    {
        std::lock_guard<std::mutex> guard{mutex};
        was_empty = queue.empty() && processing == 0;
        if (!stop)
        {
            if (pos == QueuePos::Front)
                queue.emplace_front(std::move(job), name);
            else
            {
                chassert(pos == QueuePos::Back);
                queue.emplace_back(std::move(job), name);
            }
            enqueue_count++;
        }
        max_queue_len = std::max<UInt64>(max_queue_len, queue.size());
    }
    if (was_empty)
        cv.notify_one();
}

void JobQueue::requestStop()
{
    std::lock_guard<std::mutex> guard{mutex};
    if (!stop)
    {
        stop = true;
        cv.notify_all();
    }
}

void JobQueue::process()
{
    std::unique_lock<std::mutex> lock{mutex};
    while (!stop)
    {
        if (queue.empty())
            cv.wait(lock);
        else
        {
            auto entry = std::move(queue.front());
            queue.pop_front();
            processing++;
            lock.unlock();

            auto exit_code = runJob(entry);

            lock.lock();
            processing--;

            switch (exit_code)
            {
                case JobExitCode::Reschedule: {
                    queue.emplace_back(std::move(entry));
                    if (queue.size() == 1)
                    {
                        ScopedUnlock<std::mutex> unlock{lock};
                        std::this_thread::yield();
                    }
                    break;
                }
                case JobExitCode::Done: {
                    jobs_done++;
                    if (entry.reschedule_count > kHighRescheduleReportRate)
                        jobs_high_reschedule++;
                    reschedules += entry.reschedule_count;
                    break;
                }
            }
        }
    }
}

JobExitCode JobQueue::runJob(QueueEntry & entry)
{
    auto safe_job = [this, &entry] {
        try
        {
            return entry.job();
        }
        catch (const std::exception & ex)
        {
            LOG_ERROR(log, "Exception thrown from the job: {}", ex.what());
            throw;
        }
        catch (...)
        {
            LOG_ERROR(log, "Unknown exception thrown from the job");
            throw;
        }
    };

    auto exit_code = safe_job();
    switch (exit_code)
    {
        case JobExitCode::Reschedule: {
            entry.reschedule_count++;
            if (entry.reschedule_count >= kHighRescheduleCount && entry.reschedule_count % kHighRescheduleReportRate == 0)
                LOG_DEBUG(log, "job '{}' rescheduled {} times", entry.name, entry.reschedule_count);
            break;
        }
        case JobExitCode::Done:
            entry.job = Job{};
            break;
    }

    return exit_code;
}

UInt64 JobQueue::finish()
{
    std::unique_lock<std::mutex> lock{mutex};
    while (processing != 0 || !queue.empty())
    {
        ScopedUnlock<std::mutex> unlock{lock};
        std::this_thread::yield();
    }
    return enqueue_count;
}

std::unique_ptr<JobScheduler>
createOrderedThreadPoolJobScheduler(UInt32 reader_threads, UInt32 writer_threads, UInt32 req_order_shard_power)
{
    return std::make_unique<OrderedThreadPoolJobScheduler>(reader_threads, writer_threads, req_order_shard_power);
}

std::unique_ptr<JobScheduler> createFiberRequestScheduler(
    size_t reader_threads,
    size_t writer_threads,
    size_t max_concurrent_reads,
    size_t max_concurrent_writes,
    size_t stack_size,
    size_t num_shards_power)
{
    return std::make_unique<FiberRequestScheduler>(
        reader_threads, writer_threads, max_concurrent_reads, max_concurrent_writes, stack_size, num_shards_power);
}

ThreadPoolExecutor::ThreadPoolExecutor(UInt32 num_threads, std::string_view name_) : name{name_}, queues(num_threads)
{
    chassert(num_threads > 0u);
    workers.reserve(num_threads);
    for (UInt32 i = 0; i < num_threads; i++)
    {
        queues[i] = std::make_unique<JobQueue>();
        workers.emplace_back([&q = queues[i], thread_name = fmt::format("nvmC_{}_{}", name.substr(0, 6), i)] {
            setThreadName(thread_name.c_str());
            q->process();
        });
    }
}

void ThreadPoolExecutor::enqueue(Job job, std::string_view name_, JobQueue::QueuePos pos)
{
    auto index = next_queue.fetch_add(1, std::memory_order_relaxed);
    queues[index % queues.size()]->enqueue(std::move(job), name_, pos);
}

void ThreadPoolExecutor::enqueueWithKey(Job job, std::string_view name_, JobQueue::QueuePos pos, UInt64 key)
{
    queues[key % queues.size()]->enqueue(std::move(job), name_, pos);
}

UInt64 ThreadPoolExecutor::finish()
{
    UInt64 ec = 0;
    for (auto & q : queues)
        ec += q->finish();
    return ec;
}

void ThreadPoolExecutor::join()
{
    for (auto & q : queues)
        q->requestStop();
    LOG_INFO(log, "JobScheduler: join threads for {}", name);
    for (auto & t : workers)
        t.join();
    LOG_INFO(log, "JobScheduler: join threads for {} successful", name);
    workers.clear();
}

ThreadPoolJobScheduler::ThreadPoolJobScheduler(UInt32 reader_threads, UInt32 writer_threads)
    : reader(reader_threads, "reader_pool"), writer(writer_threads, "write_pool")
{
}

void ThreadPoolJobScheduler::enqueue(Job job, std::string_view name, JobType type)
{
    switch (type)
    {
        case JobType::Read:
            reader.enqueue(std::move(job), name, JobQueue::QueuePos::Back);
            break;
        case JobType::Write:
            writer.enqueue(std::move(job), name, JobQueue::QueuePos::Back);
            break;
        case JobType::Reclaim:
            writer.enqueue(std::move(job), name, JobQueue::QueuePos::Front);
            break;
        case JobType::Flush:
            writer.enqueue(std::move(job), name, JobQueue::QueuePos::Front);
            break;
        default:
            LOG_ERROR(log, "JobScheduler: unrecognized job type: {}", static_cast<UInt32>(type));
            chassert(false);
    }
}

void ThreadPoolJobScheduler::enqueueWithKey(Job job, std::string_view name, JobType type, uint64_t key)
{
    switch (type)
    {
        case JobType::Read:
            reader.enqueueWithKey(std::move(job), name, JobQueue::QueuePos::Back, key);
            break;
        case JobType::Write:
            writer.enqueueWithKey(std::move(job), name, JobQueue::QueuePos::Back, key);
            break;
        case JobType::Reclaim:
            writer.enqueueWithKey(std::move(job), name, JobQueue::QueuePos::Front, key);
            break;
        case JobType::Flush:
            writer.enqueueWithKey(std::move(job), name, JobQueue::QueuePos::Front, key);
            break;
        default:
            LOG_ERROR(log, "JobScheduler: unrecognized job type: {}", static_cast<UInt32>(type));
            chassert(false);
    }
}

void ThreadPoolJobScheduler::join()
{
    reader.join();
    writer.join();
}

void ThreadPoolJobScheduler::finish()
{
    UInt64 enqueue_total_count = 0;
    while (true)
    {
        const uint64_t ec = reader.finish() + writer.finish();

        if (ec == enqueue_total_count)
            break;
        enqueue_total_count = ec;
    }
}

namespace
{
    constexpr size_t numShards(size_t power) { return 1ULL << power; }
}

OrderedThreadPoolJobScheduler::OrderedThreadPoolJobScheduler(size_t reader_threads, size_t writer_threads, size_t num_shards_power_)
    : mutexes(numShards(num_shards_power_))
    , pending_jobs(numShards(num_shards_power_))
    , should_spool(numShards(num_shards_power_), false)
    , num_shards_power(num_shards_power_)
    , scheduler(reader_threads, writer_threads)
{
}

void OrderedThreadPoolJobScheduler::enqueueWithKey(Job job, std::string_view name, JobType type, UInt64 key)
{
    const auto shard = key % numShards(num_shards_power);
    JobParams params{std::move(job), type, name, key};
    std::lock_guard<std::mutex> l(mutexes[shard]);
    if (should_spool[shard])
        pending_jobs[shard].emplace_back(std::move(params));
    else
    {
        should_spool[shard] = true;
        scheduleJobLocked(std::move(params), shard);
    }
}

void OrderedThreadPoolJobScheduler::scheduleJobLocked(JobParams params, UInt64 shard)
{
    scheduler.enqueueWithKey(
        [this, j = std::move(params.job), shard]() mutable {
            auto ret = j();
            if (ret == JobExitCode::Done)
                scheduleNextJob(shard);
            else
                chassert(ret == JobExitCode::Reschedule);
            return ret;
        },
        params.name,
        params.type,
        params.key);
}

void OrderedThreadPoolJobScheduler::scheduleNextJob(UInt64 shard)
{
    std::lock_guard<std::mutex> l(mutexes[shard]);
    if (pending_jobs[shard].empty())
    {
        should_spool[shard] = false;
        return;
    }

    scheduleJobLocked(std::move(pending_jobs[shard].front()), shard);
    pending_jobs[shard].pop_front();
}

void OrderedThreadPoolJobScheduler::enqueue(Job job, std::string_view name, JobType type)
{
    scheduler.enqueue(std::move(job), name, type);
}

void OrderedThreadPoolJobScheduler::finish()
{
    scheduler.finish();
}


FiberRequestDispatcher::FiberRequestDispatcher(
    JobScheduler & scheduler_, std::string_view name_, size_t max_outstanding_, size_t stack_size_)
    : scheduler(scheduler_), name(name_), max_outstanding(max_outstanding_), worker{name, FiberThread::Options(stack_size_)}
{
    worker.addTaskRemote([this]() { LOG_INFO(log, "[{}] Starting with max outstanding {}", getName(), max_outstanding); });
}

/*
 * Request dispatch loop
 *
 * The loop function is run by a fiber which is started when the first request
 * is arrived. Once started, the loop claims the incoming request queue by
 * extracting the head with replacing it with sentinel value (i.e., reserved
 * constant pointer value of 0x1) such that no other dispatcher loop can start.
 * The loop will keep polling the incoming request queue until no other requests
 * arrived while processing them. If no more requests arrived, the loop will
 * remove the sentinel value to indicate that the queue has been emptied.
 *
 * Since the request queue is a singly linked list in reverse order of arrival,
 * the queue is reversed to make sure FIFO order is preserved before dispatched.
 *
 * The loop can pause processing if the outstanding requests reached the limit.
 * Once paused, the loop will wait for the completion of any of the current
 * outstanding requests before resuming dispatches.
 */
void FiberRequestDispatcher::processLoop()
{
    // The sentinel used to claim the queue
    const auto sentinel = reinterpret_cast<FiberRequest *>(1);
    FiberRequest * incoming = nullptr;
    do
    {
        // Claim the queue so that no new dispatcher loop is started
        // while we are working on those submitted
        incoming = __atomic_exchange_n(&incoming_reqs, sentinel, __ATOMIC_ACQ_REL);

        // Inverse the incoming list to submit in FIFO order
        FiberRequest * pending = nullptr;
        while (incoming && incoming != sentinel)
        {
            auto * next = incoming->next;
            incoming->next = pending;
            pending = incoming;
            incoming = next;
        }

        while (pending)
        {
            std::unique_ptr<FiberRequest> req(pending);
            pending = pending->next;
            req->next = nullptr;

            // Enforce the maximum concurrent requests outstanding
            if (cur_outstanding.load(std::memory_order_relaxed) == max_outstanding)
            {
                // We are reusing the baton, so needs to be reset before use.
                // Note that we are supposed to be woken up by another fiber
                // running on the same thread
                baton.reset();
                baton.wait();
                chassert(cur_outstanding.load(std::memory_order_relaxed) < max_outstanding);
            }
            // Dispatch the Request
            scheduleReq(std::move(req));
        }

        // Try to unclaim the incoming_reqs queue
        // If the head is not sentinel as expected, it means that some new requests
        // have been arrived while dispatching previous ones
        incoming = sentinel;
    } while (!__atomic_compare_exchange_n(&incoming_reqs, &incoming, nullptr, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED));
}

void FiberRequestDispatcher::scheduleReq(std::unique_ptr<FiberRequest> req)
{
    // Start a new fiber running the given request
    cur_outstanding.fetch_add(1, std::memory_order_relaxed);
    worker.addTask([this, rq = std::move(req)]() mutable {
        while (rq->execute() == JobExitCode::Reschedule)
        {
            folly::fibers::yield();
        }

        auto key = rq->getKey();
        // Release rq to destruct it
        rq.reset();

        scheduler.notifyCompletion(key);

        num_completed.fetch_add(1, std::memory_order_relaxed);
        cur_outstanding.fetch_sub(1, std::memory_order_relaxed);
        if (cur_outstanding.load(std::memory_order_relaxed) + 1 == max_outstanding)
        {
            baton.post();
        }
    });
}

void FiberRequestDispatcher::submitReq(std::unique_ptr<FiberRequest> fiber_req)
{
    chassert(!!fiber_req);
    num_submitted.fetch_add(1, std::memory_order_relaxed);

    auto * req = fiber_req.release();
    FiberRequest * old_value = nullptr;

    // Add the list to the head of the list. If the head was nullptr,
    // it means there is no dispatcher task is running, when a new one
    // needs to be started
    bool status = atomicUpdateValue(
        &incoming_reqs,
        &old_value,
        [](FiberRequest *) { return true; },
        [&req](FiberRequest * cur) {
            req->next = cur;
            return req;
        });
    chassert(status);

    if (!old_value)
    {
        // We were the first one submitting to the queue, so start a fiber
        // running the dispatch loop
        worker.addTaskRemote([this]() { processLoop(); });
    }
}

UInt64 FiberRequestDispatcher::finish()
{
    while (getNumCompleted() < getNumSubmitted())
    {
        folly::fibers::yield();
    }
    return getNumCompleted();
}

FiberRequestScheduler::FiberRequestScheduler(
    size_t reader_threads,
    size_t writer_threads,
    size_t max_concurrent_reads,
    size_t max_concurrent_writes,
    size_t stack_size,
    size_t num_shards_power)
    : num_reader_threads(reader_threads)
    , num_writer_threads(writer_threads)
    , num_shards(1ULL << num_shards_power)
    , mutexes(num_shards)
    , pending_reqs(num_shards)
    , should_spool(num_shards, false)
{
    // Adjust the max number of reads and writes if needed
    max_concurrent_reads = std::max(max_concurrent_reads, reader_threads);
    max_concurrent_writes = std::max(max_concurrent_writes, writer_threads);

    for (size_t i = 0; i < num_reader_threads; i++)
    {
        auto dispatcher = std::make_shared<FiberRequestDispatcher>(
            *this, fmt::format("fiber_reader_{}", i), max_concurrent_reads / num_reader_threads, stack_size);
        reader_dispatchers.emplace_back(std::move(dispatcher));
    }

    for (size_t i = 0; i < num_writer_threads; i++)
    {
        auto dispatcher = std::make_shared<FiberRequestDispatcher>(
            *this, fmt::format("fiber_writer_{}", i), max_concurrent_writes / num_writer_threads, stack_size);
        writer_dispatchers.emplace_back(std::move(dispatcher));
    }
}

FiberRequestScheduler::~FiberRequestScheduler()
{
    stopped = true;
    reader_dispatchers.clear();
    writer_dispatchers.clear();
}

void FiberRequestScheduler::enqueue(Job job, std::string_view name, JobType type)
{
    auto key = next_key.fetch_add(1, std::memory_order_relaxed);
    enqueueWithKey(job, name, type, key);
}

void FiberRequestScheduler::enqueueWithKey(Job job, std::string_view name, JobType type, uint64_t key)
{
    if (stopped)
        return;

    num_enqueued++;

    auto req = std::make_unique<FiberRequest>(std::move(job), name, type, key);
    // Allow one request can be outstanding per shard by spooling requests
    // if there is another request already running
    const auto shard = req->getKey() % num_shards;
    std::lock_guard<folly::fibers::TimedMutex> l(mutexes[shard]);
    if (should_spool[shard])
    {
        pending_reqs[shard].emplace_back(std::move(req));
    }
    else
    {
        should_spool[shard] = true;
        auto & dispatcher = getDispatcher(req->getKey(), req->getType());
        dispatcher.submitReq(std::move(req));
    }
}

// Notify completion of the request
void FiberRequestScheduler::notifyCompletion(uint64_t key)
{
    const auto shard = key % num_shards;
    std::lock_guard<folly::fibers::TimedMutex> l(mutexes[shard]);
    if (pending_reqs[shard].empty())
    {
        should_spool[shard] = false;
        return;
    }

    auto next_req = std::move(pending_reqs[shard].front());
    if (!stopped)
    {
        auto & dispatcher = getDispatcher(next_req->getKey(), next_req->getType());
        dispatcher.submitReq(std::move(next_req));
    }
    pending_reqs[shard].pop_front();
}

void FiberRequestScheduler::finish()
{
    UInt64 total_finished = 0;
    while (num_enqueued.load() != total_finished)
    {
        total_finished = 0;
        for (auto & i : reader_dispatchers)
            total_finished += i->finish();
        for (auto & i : writer_dispatchers)
            total_finished += i->finish();
    }
}

FiberRequestDispatcher & FiberRequestScheduler::getDispatcher(uint64_t keyHash, JobType type)
{
    if (type == JobType::Read)
    {
        return *reader_dispatchers[keyHash % num_reader_threads];
    }

    // Reclaim and Flush job should not be submitted to this scheduler
    if (type != JobType::Write)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Reclaim or Flush jobs are not allowed to be submitted to FiberRequestDispatcher, Jobtype = {}",
            type);

    chassert(type == JobType::Write);
    return *writer_dispatchers[keyHash % num_writer_threads];
}

}
