#include <mutex>
#include <random>
#include <set>
#include <thread>

#include <gtest/gtest.h>

#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/tests/SeqPoints.h>
#include <Common/thread_local_rng.h>

namespace DB::HybridCache
{
void spinWait(std::atomic<int> & ai, int target)
{
    while (ai.load(std::memory_order_acquire) != target)
        std::this_thread::yield();
}


TEST(ThreadPoolJobScheduler, BlockOneTaskTwoWorkers)
{
    std::atomic<int> ai{0};
    ThreadPoolJobScheduler scheduler{1, 2};
    scheduler.enqueue(
        [&ai]() {
            spinWait(ai, 2);
            ai.store(3, std::memory_order_release);
            return JobExitCode::Done;
        },
        "wait",
        JobType::Write);
    scheduler.enqueue(
        [&ai]() {
            ai.store(1, std::memory_order_release);
            return JobExitCode::Done;
        },
        "post",
        JobType::Write);
    spinWait(ai, 1);
    ai.store(2, std::memory_order_release);
    spinWait(ai, 3);
}

TEST(ThreadPoolJobScheduler, StopEmpty)
{
    ThreadPoolJobScheduler scheduler{1, 1};
}

TEST(ThreadPoolJobScheduler, EnqueueFirst)
{
    ThreadPoolJobScheduler scheduler{1, 1};
    for (int i = 0; i < 50; i++)
    {
        std::vector<int> v;
        scheduler.enqueue(
            [&scheduler, &v]() {
                v.push_back(0);
                for (int j = 1; j < 3; j++)
                {
                    scheduler.enqueue(
                        [&v, j]() {
                            v.push_back(j);
                            return JobExitCode::Done;
                        },
                        "write1",
                        JobType::Write);
                }
                scheduler.enqueue(
                    [&v]() {
                        v.push_back(3);
                        return JobExitCode::Done;
                    },
                    "reclaim",
                    JobType::Reclaim);
                return JobExitCode::Done;
            },
            "write2",
            JobType::Write);
        scheduler.finish();
        std::vector<int> expected{0, 3, 1, 2};
        EXPECT_EQ(expected, v);
    }
}

TEST(ThreadPoolJobScheduler, EnqueueWithKey)
{
    ThreadPoolJobScheduler scheduler{1, 2};
    for (int i = 0; i < 50; i++)
    {
        std::set<std::thread::id> thread_ids;
        for (int j = 0; j < 3; j++)
        {
            scheduler.enqueueWithKey(
                [&thread_ids]() {
                    thread_ids.insert(std::this_thread::get_id());
                    return JobExitCode::Done;
                },
                "test",
                JobType::Write,
                0);
        }
        scheduler.finish();
        EXPECT_EQ(1, thread_ids.size());
    }
}

TEST(ThreadPoolJobScheduler, Finish)
{
    SeqPoints sp;
    ThreadPoolJobScheduler scheduler{2, 1};
    bool done = false;
    bool sp_reached = false;
    scheduler.enqueue(
        [&sp, &done, &sp_reached]() {
            if (sp_reached)
            {
                done = true;
                return JobExitCode::Done;
            }
            sp.reached(0);
            return JobExitCode::Reschedule;
        },
        "test",
        JobType::Read);

    sp.wait(0);
    sp_reached = true;

    scheduler.finish();
    EXPECT_TRUE(done);
}

TEST(ThreadPoolJobScheduler, FinishSchedulesNew)
{
    SeqPoints sp;

    ThreadPoolJobScheduler scheduler{2, 1};

    scheduler.enqueueWithKey(
        [&sp, &scheduler] {
            sp.wait(0);
            scheduler.enqueueWithKey(
                [&sp] {
                    EXPECT_FALSE(sp.waitFor(1, std::chrono::seconds{1}));
                    return JobExitCode::Done;
                },
                "job2",
                JobType::Read,
                0);
            return JobExitCode::Done;
        },
        "job1",
        JobType::Read,
        1);

    std::thread t{[&sp] {
        std::this_thread::sleep_for(std::chrono::seconds{1});
        sp.reached(0);
    }};
    scheduler.finish();
    sp.reached(1);
    t.join();
}

TEST(ThreadPoolJobScheduler, ReadWriteReclaim)
{
    std::vector<int> v;
    ThreadPoolJobScheduler scheduler{1, 1};

    SeqPoints sp;
    sp.setName(0, "Write issued");
    sp.setName(1, "Reclaim issued");
    sp.setName(2, "Read issued");
    scheduler.enqueue(
        [&sp, &scheduler, &v] {
            sp.wait(0);
            v.push_back(0);
            scheduler.enqueue(
                [&sp, &v] {
                    sp.wait(2);
                    v.push_back(1);
                    return JobExitCode::Done;
                },
                "write2",
                JobType::Write);
            sp.reached(2);
            return JobExitCode::Done;
        },
        "read1",
        JobType::Read);
    scheduler.enqueue(
        [&sp, &v] {
            if (!sp.waitFor(1, std::chrono::milliseconds{10}))
                return JobExitCode::Reschedule;
            v.push_back(2);
            sp.reached(0);
            return JobExitCode::Done;
        },
        "write1",
        JobType::Write);
    scheduler.enqueue(
        [&sp, &v] {
            v.push_back(3);
            sp.reached(1);
            return JobExitCode::Done;
        },
        "reclaim",
        JobType::Write);
    scheduler.finish();
    EXPECT_EQ((std::vector<int>{3, 2, 0, 1}), v);
}

TEST(ThreadPoolJobScheduler, MaxQueueLen)
{
    unsigned int num_queues = 4;
    ThreadPoolJobScheduler scheduler{num_queues, 1};
    SeqPoints sp;
    sp.setName(0, "all enqueued");

    std::atomic<unsigned int> jobs_done{0};
    auto job = [&] {
        sp.wait(0);
        ++jobs_done;
        return JobExitCode::Done;
    };

    int num_to_queue = 1000;
    for (int i = 0; i < num_to_queue; i++)
        scheduler.enqueue(job, "read", JobType::Read);


    sp.reached(0);
    scheduler.finish();
    EXPECT_EQ(jobs_done, num_to_queue);
}

TEST(OrderedThreadPoolJobScheduler, OrderedEnqueueSameType)
{
    UInt64 key = 5;
    SeqPoints sp;
    std::vector<int> order;
    int seq = 0;
    OrderedThreadPoolJobScheduler scheduler{1, 2, 2};
    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(1);
            return JobExitCode::Done;
        },
        "",
        JobType::Write,
        key);

    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(2);
            return JobExitCode::Done;
        },
        "",
        JobType::Write,
        key);

    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(3);
            return JobExitCode::Done;
        },
        "",
        JobType::Write,
        key);

    sp.reached(0);
    sp.wait(1);
    sp.wait(2);
    sp.wait(3);

    for (int i = 1; i <= seq; i++)
        EXPECT_EQ(i, order[i - 1]);
}

TEST(OrderedThreadPoolJobScheduler, OrderedEnqueueDiffType)
{
    std::uniform_int_distribution<UInt32> dist;
    std::array<JobType, 2> job_types = {JobType::Read, JobType::Write};
    UInt64 key = 5;
    SeqPoints sp;
    std::vector<int> order;
    int seq = 0;
    OrderedThreadPoolJobScheduler scheduler{1, 2, 2};
    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(1);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(2);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(3);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    sp.reached(0);
    sp.wait(1);
    sp.wait(2);
    sp.wait(3);

    for (int i = 1; i <= seq; i++)
    {
        EXPECT_EQ(i, order[i - 1]);
    }
}

TEST(OrderedThreadPoolJobScheduler, SpoolAndFinish)
{
    std::uniform_int_distribution<UInt32> dist;
    std::array<JobType, 2> job_types = {JobType::Read, JobType::Write};
    uint64_t key = 5;
    SeqPoints sp;

    OrderedThreadPoolJobScheduler scheduler{1, 2, 2};
    scheduler.enqueueWithKey(
        [&sp]() {
            sp.wait(0);
            sp.reached(1);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&sp]() {
            sp.wait(0);
            sp.reached(2);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&sp, i = 0]() mutable {
            sp.wait(0);
            if (i < 2)
            {
                i++;
                return JobExitCode::Reschedule;
            }
            sp.reached(3);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);


    sp.reached(0);
    scheduler.finish();
    sp.wait(1);
    sp.wait(2);
    sp.wait(3);
}

TEST(OrderedThreadPoolJobScheduler, JobWithRetry)
{
    std::uniform_int_distribution<UInt32> dist;
    std::array<JobType, 3> job_types = {JobType::Read, JobType::Write, JobType::Reclaim};
    UInt64 key = 5;
    SeqPoints sp;

    std::atomic<UInt64> num_reschedules{0};

    OrderedThreadPoolJobScheduler scheduler{1, 2, 2};
    scheduler.enqueueWithKey(
        [&, i = 0]() mutable {
            sp.wait(0);
            if (i < 2)
            {
                i++;
                num_reschedules++;
                return JobExitCode::Reschedule;
            }
            sp.reached(1);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&, i = 0]() mutable {
            sp.wait(0);
            if (i < 2)
            {
                i++;
                num_reschedules++;
                return JobExitCode::Reschedule;
            }
            sp.reached(2);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&, i = 0]() mutable {
            sp.wait(0);
            if (i < 2)
            {
                i++;
                num_reschedules++;
                return JobExitCode::Reschedule;
            }
            sp.reached(3);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    EXPECT_EQ(0, num_reschedules);

    sp.reached(0);
    sp.wait(1);
    EXPECT_GE(num_reschedules, 2);
    sp.wait(2);
    EXPECT_GE(num_reschedules, 4);
    sp.wait(3);
    EXPECT_EQ(6, num_reschedules);
}

TEST(OrderedThreadPoolJobScheduler, OrderedEnqueueAndFinish)
{
    std::uniform_int_distribution<UInt32> dist;
    unsigned int num_keys = 10000;
    std::atomic<int> num_completed{0};

    {
        OrderedThreadPoolJobScheduler scheduler{3, 32, 10};
        for (unsigned int i = 0; i < num_keys; i++)
        {
            scheduler.enqueueWithKey(
                [&]() {
                    ++num_completed;
                    return JobExitCode::Done;
                },
                "",
                JobType::Write,
                dist(thread_local_rng));
        }

        scheduler.finish();
    }
    EXPECT_EQ(num_completed, num_keys);
}

TEST(OrderedThreadPoolJobScheduler, OrderedEnqueueMaxLen)
{
    std::uniform_int_distribution<UInt32> dist;
    unsigned int num_keys = 10000;
    std::atomic<int> num_completed{0};
    SeqPoints sp;
    sp.setName(0, "all enqueued");

    unsigned int num_queues = 4;
    OrderedThreadPoolJobScheduler scheduler{num_queues, 1, 10};
    for (unsigned int i = 0; i < num_keys; i++)
    {
        scheduler.enqueueWithKey(
            [&]() {
                sp.wait(0);
                ++num_completed;
                return JobExitCode::Done;
            },
            "",
            JobType::Read,
            dist(thread_local_rng));
    }

    sp.reached(0);
    scheduler.finish();
    EXPECT_EQ(num_completed, num_keys);
}

TEST(FiberRequestScheduler, BlockOneTaskTwoWorkers)
{
    std::atomic<int> ai{0};
    FiberRequestScheduler scheduler{1, 2, 2, 4, 0, 10};
    scheduler.enqueue(
        [&ai]() {
            spinWait(ai, 2);
            ai.store(3, std::memory_order_release);
            return JobExitCode::Done;
        },
        "wait",
        JobType::Write);
    scheduler.enqueue(
        [&ai]() {
            ai.store(1, std::memory_order_release);
            return JobExitCode::Done;
        },
        "post",
        JobType::Write);
    spinWait(ai, 1);
    ai.store(2, std::memory_order_release);
    spinWait(ai, 3);
}

TEST(FiberRequestScheduler, StopEmpty)
{
    FiberRequestScheduler scheduler{1, 1, 0, 0, 0, 10};
}

TEST(FiberRequestScheduler, ReclainOrFlushJob)
{
    FiberRequestScheduler scheduler{1, 1, 0, 0, 0, 10};

    EXPECT_THROW({ scheduler.enqueue([]() { return JobExitCode::Done; }, "reclaim", JobType::Reclaim); }, Exception);
    EXPECT_THROW({ scheduler.enqueue([]() { return JobExitCode::Done; }, "flush", JobType::Flush); }, Exception);
}

TEST(FiberRequestScheduler, EnqueueWithKey)
{
    FiberRequestScheduler scheduler{1, 2, 10, 10, 0, 10};
    for (int i = 0; i < 50; i++)
    {
        folly::fibers::TimedMutex mutex;
        std::set<std::thread::id> thread_ids;
        for (int j = 0; j < 3; j++)
        {
            scheduler.enqueueWithKey(
                [&thread_ids, &mutex]() {
                    std::lock_guard<folly::fibers::TimedMutex> guard{mutex};
                    thread_ids.insert(std::this_thread::get_id());
                    return JobExitCode::Done;
                },
                "test",
                JobType::Write,
                0);
        }
        scheduler.finish();
        EXPECT_EQ(1, thread_ids.size());
    }
}

TEST(FiberRequestScheduler, Finish)
{
    SeqPoints sp;
    FiberRequestScheduler scheduler{2, 1, 10, 10, 0, 10};
    bool done = false;
    bool sp_reached = false;
    scheduler.enqueue(
        [&sp, &done, &sp_reached]() {
            if (sp_reached)
            {
                done = true;
                return JobExitCode::Done;
            }
            sp.reached(0);
            return JobExitCode::Reschedule;
        },
        "test",
        JobType::Read);

    sp.wait(0);
    sp_reached = true;

    scheduler.finish();
    EXPECT_TRUE(done);
}

TEST(FiberRequestScheduler, FinishSchedulesNew)
{
    SeqPoints sp;

    FiberRequestScheduler scheduler{2, 1, 10, 10, 0, 10};

    scheduler.enqueueWithKey(
        [&sp, &scheduler] {
            sp.wait(0);
            scheduler.enqueueWithKey(
                [&sp] {
                    EXPECT_FALSE(sp.waitFor(1, std::chrono::seconds{1}));
                    return JobExitCode::Done;
                },
                "job2",
                JobType::Read,
                0);
            return JobExitCode::Done;
        },
        "job1",
        JobType::Read,
        1);

    std::thread t{[&sp] {
        std::this_thread::sleep_for(std::chrono::seconds{1});
        sp.reached(0);
    }};
    scheduler.finish();
    sp.reached(1);
    t.join();
}

TEST(FiberRequestScheduler, ReadWriteReclaim)
{
    std::vector<int> v;
    FiberRequestScheduler scheduler{1, 1,4,4,0,10};

    SeqPoints sp;
    sp.setName(0, "Write issued");
    sp.setName(1, "Reclaim issued");
    sp.setName(2, "Read issued");
    scheduler.enqueue(
        [&sp, &scheduler, &v] {
            sp.wait(0);
            v.push_back(0);
            scheduler.enqueue(
                [&sp, &v] {
                    sp.wait(2);
                    v.push_back(1);
                    return JobExitCode::Done;
                },
                "write2",
                JobType::Write);
            sp.reached(2);
            return JobExitCode::Done;
        },
        "read1",
        JobType::Read);
    scheduler.enqueue(
        [&sp, &v] {
            if (!sp.waitFor(1, std::chrono::milliseconds{10}))
                return JobExitCode::Reschedule;
            v.push_back(2);
            sp.reached(0);
            return JobExitCode::Done;
        },
        "write1",
        JobType::Write);
    scheduler.enqueue(
        [&sp, &v] {
            v.push_back(3);
            sp.reached(1);
            return JobExitCode::Done;
        },
        "reclaim",
        JobType::Write);
    scheduler.finish();
    EXPECT_EQ((std::vector<int>{3, 2, 0, 1}), v);
}

TEST(FiberRequestScheduler, MaxQueueLen)
{
    unsigned int num_queues = 4;
    FiberRequestScheduler scheduler{num_queues, 1, 16, 16, 0, 10};
    SeqPoints sp;
    sp.setName(0, "all enqueued");

    std::atomic<unsigned int> jobs_done{0};
    auto job = [&] {
        sp.wait(0);
        ++jobs_done;
        return JobExitCode::Done;
    };

    int num_to_queue = 1000;
    for (int i = 0; i < num_to_queue; i++)
        scheduler.enqueue(job, "read", JobType::Read);


    sp.reached(0);
    scheduler.finish();
    EXPECT_EQ(jobs_done, num_to_queue);
}

TEST(FiberRequestScheduler, OrderedEnqueueSameType)
{
    UInt64 key = 5;
    SeqPoints sp;
    std::vector<int> order;
    int seq = 0;
    FiberRequestScheduler scheduler{1, 2, 4, 4, 0, 2};
    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(1);
            return JobExitCode::Done;
        },
        "",
        JobType::Write,
        key);

    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(2);
            return JobExitCode::Done;
        },
        "",
        JobType::Write,
        key);

    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(3);
            return JobExitCode::Done;
        },
        "",
        JobType::Write,
        key);

    sp.reached(0);
    sp.wait(1);
    sp.wait(2);
    sp.wait(3);

    for (int i = 1; i <= seq; i++)
        EXPECT_EQ(i, order[i - 1]);
}

TEST(FiberRequestScheduler, OrderedEnqueueDiffType)
{
    std::uniform_int_distribution<UInt32> dist;
    std::array<JobType, 2> job_types = {JobType::Read, JobType::Write};
    UInt64 key = 5;
    SeqPoints sp;
    std::vector<int> order;
    int seq = 0;
    FiberRequestScheduler scheduler{1, 2, 4, 4, 0, 2};
    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(1);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(2);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&sp, &order, n = ++seq]() {
            sp.wait(0);
            order.push_back(n);
            sp.reached(3);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    sp.reached(0);
    sp.wait(1);
    sp.wait(2);
    sp.wait(3);

    for (int i = 1; i <= seq; i++)
    {
        EXPECT_EQ(i, order[i - 1]);
    }
}

TEST(FiberRequestScheduler, SpoolAndFinish)
{
    std::uniform_int_distribution<UInt32> dist;
    std::array<JobType, 2> job_types = {JobType::Read, JobType::Write};
    uint64_t key = 5;
    SeqPoints sp;

    FiberRequestScheduler scheduler{1, 2, 4, 4, 0, 2};
    scheduler.enqueueWithKey(
        [&sp]() {
            sp.wait(0);
            sp.reached(1);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&sp]() {
            sp.wait(0);
            sp.reached(2);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&sp, i = 0]() mutable {
            sp.wait(0);
            if (i < 2)
            {
                i++;
                return JobExitCode::Reschedule;
            }
            sp.reached(3);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);


    sp.reached(0);
    scheduler.finish();
    sp.wait(1);
    sp.wait(2);
    sp.wait(3);
}

TEST(FiberRequestScheduler, JobWithRetry)
{
    std::uniform_int_distribution<UInt32> dist;
    std::array<JobType, 3> job_types = {JobType::Read, JobType::Write};
    UInt64 key = 5;
    SeqPoints sp;

    std::atomic<UInt64> num_reschedules{0};

    FiberRequestScheduler scheduler{1, 2, 4, 4, 0, 2};
    scheduler.enqueueWithKey(
        [&, i = 0]() mutable {
            sp.wait(0);
            if (i < 2)
            {
                i++;
                num_reschedules++;
                return JobExitCode::Reschedule;
            }
            sp.reached(1);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&, i = 0]() mutable {
            sp.wait(0);
            if (i < 2)
            {
                i++;
                num_reschedules++;
                return JobExitCode::Reschedule;
            }
            sp.reached(2);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    scheduler.enqueueWithKey(
        [&, i = 0]() mutable {
            sp.wait(0);
            if (i < 2)
            {
                i++;
                num_reschedules++;
                return JobExitCode::Reschedule;
            }
            sp.reached(3);
            return JobExitCode::Done;
        },
        "",
        job_types[dist(thread_local_rng) % job_types.size()],
        key);

    EXPECT_EQ(0, num_reschedules);

    sp.reached(0);
    sp.wait(1);
    EXPECT_GE(num_reschedules, 2);
    sp.wait(2);
    EXPECT_GE(num_reschedules, 4);
    sp.wait(3);
    EXPECT_EQ(6, num_reschedules);
}

TEST(FiberRequestScheduler, OrderedEnqueueAndFinish)
{
    std::uniform_int_distribution<UInt32> dist;
    unsigned int num_keys = 10000;
    std::atomic<int> num_completed{0};

    {
        FiberRequestScheduler scheduler{4, 32, 256, 256, 0, 10};
        for (unsigned int i = 0; i < num_keys; i++)
        {
            scheduler.enqueueWithKey(
                [&]() {
                    ++num_completed;
                    return JobExitCode::Done;
                },
                "",
                JobType::Write,
                dist(thread_local_rng));
        }

        scheduler.finish();
    }
    EXPECT_EQ(num_completed, num_keys);
}

TEST(FiberRequestScheduler, OrderedEnqueueMaxLen)
{
    std::uniform_int_distribution<UInt32> dist;
    unsigned int num_keys = 10000;
    std::atomic<int> num_completed{0};
    SeqPoints sp;
    sp.setName(0, "all enqueued");

    unsigned int num_queues = 4;
    FiberRequestScheduler scheduler{num_queues, 1, 256, 256, 0, 10};
    for (unsigned int i = 0; i < num_keys; i++)
    {
        scheduler.enqueueWithKey(
            [&]() {
                sp.wait(0);
                ++num_completed;
                return JobExitCode::Done;
            },
            "",
            JobType::Read,
            dist(thread_local_rng));
    }

    sp.reached(0);
    scheduler.finish();
    EXPECT_EQ(num_completed, num_keys);
}

}
