#pragma once

#include <deque>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

#include <fmt/core.h>

#include <Storages/DiskCache/JobScheduler.h>
#include <Common/Exception.h>
#include <common/defines.h>
#include <common/types.h>

namespace DB::HybridCache
{
class MockJobScheduler : public JobScheduler
{
public:
    MockJobScheduler() = default;
    ~MockJobScheduler() override { chassert(q.empty()); }

    void enqueue(Job job, std::string_view name, JobType type) override
    {
        std::lock_guard<std::mutex> guard{m};
        switch (type)
        {
            case JobType::Reclaim:
            case JobType::Flush:
                q.emplace_front(std::move(job), name);
                break;
            case JobType::Read:
            case JobType::Write:
                q.emplace_back(std::move(job), name);
                break;
            default:
                chassert(false);
        }
    }

    void enqueueWithKey(Job job, std::string_view name, JobType type, UInt64 /* key */) override { enqueue(std::move(job), name, type); }

    void finish() override
    {
        std::unique_lock<std::mutex> lock{m};
        while (!q.empty() || processing)
        {
            lock.unlock();
            std::this_thread::yield();
            lock.lock();
        }
    }

    bool runFirst() { return runFirstIf(""); }

    bool runFirstIf(std::string_view expected)
    {
        std::unique_lock<std::mutex> lock{m};
        if (q.empty())
            throwLogicError("empty job queue");
        return runFirstIfLocked(expected, lock);
    }

    size_t getQueueSize() const
    {
        std::lock_guard<std::mutex> guard{m};
        return q.size();
    }

    size_t getDoneCount() const
    {
        std::lock_guard<std::mutex> guard{m};
        return done_count;
    }

    size_t getRescheduleCount() const
    {
        std::lock_guard<std::mutex> guard{m};
        return reschedule_count;
    }

protected:
    struct JobName
    {
        Job job;
        std::string_view name{};

        JobName(Job j, std::string_view n) : job{std::move(j)}, name{n} { }

        bool nameIs(std::string_view expected) const { return expected.empty() || expected == name; }
    };

    [[noreturn]] static void throwLogicError(const std::string & what) { throw std::logic_error(what); }

    bool runFirstIfLocked(std::string_view expected, std::unique_lock<std::mutex> & lock)
    {
        chassert(lock.owns_lock());
        auto first = std::move(q.front());
        q.pop_front();
        if (!first.nameIs(expected))
        {
            q.push_front(std::move(first));
            throwLogicError(fmt::format("found job '{}', expected '{}'", first.name, expected));
        }
        JobExitCode ec;
        {
            processing = true;
            lock.unlock();
            ec = first.job();
            lock.lock();
            processing = false;
        }
        if (ec == JobExitCode::Done)
        {
            done_count++;
            return true;
        }
        reschedule_count++;
        q.push_back(std::move(first));
        return false;
    }

    std::deque<JobName> q;
    size_t done_count;
    size_t reschedule_count;
    std::atomic<bool> processing{false};
    mutable std::mutex m;
};

class MockSingleThreadJobScheduler : public MockJobScheduler
{
public:
    MockSingleThreadJobScheduler()
    {
        t = std::thread([this]() {
            while (!stopped)
            {
                process();
            }
        });
    }

    ~MockSingleThreadJobScheduler() override
    {
        stopped = true;
        t.join();
    }

private:
    void process()
    {
        std::unique_lock<std::mutex> lock{m};
        while (true)
        {
            if (!q.empty())
                runFirstIfLocked("", lock);
            if (q.empty() && !processing)
                break;
            lock.unlock();
            std::this_thread::yield();
            lock.lock();
        }
    }

    std::atomic<bool> stopped{false};
    std::thread t;
};
}
