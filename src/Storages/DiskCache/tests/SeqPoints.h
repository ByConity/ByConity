#pragma once

#include <Common/Logger.h>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>

#include <Poco/Logger.h>

#include <common/logger_useful.h>
#include <common/types.h>

namespace DB::HybridCache
{
class SeqPoints
{
public:
    explicit SeqPoints() = default;

    SeqPoints(const SeqPoints &) = delete;
    SeqPoints & operator=(const SeqPoints &) = delete;

    void reached(UInt32 idx)
    {
        std::lock_guard<std::mutex> guard{mutex};
        LOG_INFO(log, "Reached {} {}", idx, points[idx].name);
        points[idx].reached = true;
        cv.notify_all();
    }

    void wait(UInt32 idx)
    {
        std::unique_lock<std::mutex> lock{mutex};
        LOG_INFO(log, "Wait {} {}", idx, points[idx].name);
        while (!points[idx].reached)
            cv.wait(lock);
    }

    bool waitFor(UInt32 idx, std::chrono::microseconds dur)
    {
        auto until = std::chrono::steady_clock::now() + dur;
        std::unique_lock<std::mutex> lock{mutex};
        LOG_INFO(log, "Wait {} {}", idx, points[idx].name);
        while (!points[idx].reached)
        {
            if (cv.wait_until(lock, until) == std::cv_status::timeout)
                return false;
        }
        return true;
    }

    void setName(UInt32 idx, std::string msg)
    {
        std::lock_guard<std::mutex> guard{mutex};
        points[idx].name = std::move(msg);
    }

private:
    LoggerPtr log = getLogger("SeqPoints");

    struct Point
    {
        bool reached{false};
        std::string name{};
    };

    mutable std::mutex mutex;
    mutable std::condition_variable cv;
    std::map<UInt32, Point> points;
};
}
