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
}
