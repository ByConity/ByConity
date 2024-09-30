#include "Common/Logger.h"
#include <Common/InjectPause.h>

#include <common/logger_useful.h>

#include <folly/Indestructible.h>
#include <folly/Synchronized.h>
#include <folly/fibers/Baton.h>
#include <folly/logging/xlog.h>
#include <glog/logging.h>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>

namespace DB
{
struct BatonSet
{
    std::deque<folly::fibers::Baton *> batons;
    std::condition_variable cv;
    bool enabled{false};
    size_t limit{0};
    std::shared_ptr<PauseCallback> callback;
};

using PausePointsMap = folly::Synchronized<std::unordered_map<std::string, BatonSet>, std::mutex>;
PausePointsMap & pausePoints()
{
    static folly::Indestructible<PausePointsMap> pause_points;
    return *pause_points;
}

size_t wakeupBatonSet(BatonSet & batonSet, size_t numThreads)
{
    size_t num_waked = 0;
    if (numThreads == 0)
    {
        numThreads = batonSet.batons.size();
    }

    while (!batonSet.batons.empty() && num_waked < numThreads)
    {
        auto * b = batonSet.batons.front();
        batonSet.batons.pop_front();
        b->post();
        num_waked++;
    }

    return num_waked;
}

bool & injectPauseEnabled()
{
    static bool enabled = false;
    return enabled;
}

// Flag controls the debug logging
bool & injectPauseLogEnabled()
{
    static bool enabled = false;
    return enabled;
}

void injectPause(folly::StringPiece name)
{
    if (!injectPauseEnabled())
    {
        return;
    }
    folly::fibers::Baton baton;
    std::shared_ptr<PauseCallback> callback;
    {
        auto ptr = pausePoints().lock();
        auto it = ptr->find(name.str());
        if (it == ptr->end() || !it->second.enabled || (it->second.limit > 0 && it->second.batons.size() == it->second.limit))
        {
            if (injectPauseLogEnabled())
            {
                LOG_ERROR(getLogger("InjectPause"), "[{}] injectPause not set", name);
            }
            return;
        }
        if (injectPauseLogEnabled())
        {
            LOG_ERROR(getLogger("InjectPause"), "[{}] injectPause begin", name);
        }
        callback = it->second.callback;
        if (!callback)
        {
            it->second.batons.push_back(&baton);
            it->second.cv.notify_one();
        }
    }

    if (callback)
    {
        (*callback)();
    }
    else
    {
        baton.wait();
    }

    /* Avoid potential protect-its-own-lifetime bug:
     Wait for the post() to finish before destroying the Baton. */
    auto ptr = pausePoints().lock();
    if (injectPauseLogEnabled())
    {
        LOG_ERROR(getLogger("InjectPause"), "[{}] injectPause end", name);
    }
}

void injectPauseSet(folly::StringPiece name, size_t numThreads)
{
    if (!injectPauseEnabled())
    {
        return;
    }
    auto ptr = pausePoints().lock();
    if (injectPauseLogEnabled())
    {
        LOG_ERROR(getLogger("InjectPause"), "[{}] injectPauseSet threads {}", name, numThreads);
    }
    auto res = ptr->emplace(std::piecewise_construct, std::make_tuple(name.str()), std::make_tuple());
    res.first->second.limit = numThreads;
    res.first->second.enabled = true;
}

void injectPauseSet(folly::StringPiece name, PauseCallback && callback)
{
    if (!injectPauseEnabled())
    {
        return;
    }
    auto ptr = pausePoints().lock();
    if (injectPauseLogEnabled())
    {
        LOG_ERROR(getLogger("InjectPause"), "[{}] injectPauseSet callback", name);
    }
    auto res = ptr->emplace(std::piecewise_construct, std::make_tuple(name.str()), std::make_tuple());
    res.first->second.limit = 0;
    res.first->second.enabled = true;
    res.first->second.callback = std::make_shared<PauseCallback>(std::move(callback));
}

bool injectPauseWait(folly::StringPiece name, size_t numThreads, bool wakeup, uint32_t timeoutMs)
{
    if (!injectPauseEnabled() || !numThreads)
    {
        return false;
    }
    auto ptr = pausePoints().lock();
    if (injectPauseLogEnabled())
    {
        LOG_ERROR(getLogger("InjectPause"), "[{}] injectPauseWait begin {}", name, numThreads);
    }
    auto it = ptr->find(name.str());
    if (it == ptr->end() || !it->second.enabled)
    {
        if (injectPauseLogEnabled())
        {
            LOG_ERROR(getLogger("InjectPause"), "[{}] injectPauseWait: ERROR not set", name);
        }
        return false;
    }

    if (!timeoutMs || timeoutMs > kInjectPauseMaxWaitTimeoutMs)
    {
        timeoutMs = kInjectPauseMaxWaitTimeoutMs;
    }

    auto & baton_set = it->second;
    bool status = baton_set.cv.wait_for(ptr.as_lock(), std::chrono::milliseconds(timeoutMs), [&baton_set, numThreads]() {
        return !baton_set.enabled || baton_set.batons.size() >= numThreads;
    });

    if (status && wakeup)
    {
        wakeupBatonSet(baton_set, numThreads);
    }

    if (injectPauseLogEnabled())
    {
        std::string err_str;
        if (!status)
        {
            err_str = fmt::format(" with ERR (paused {})", baton_set.batons.size());
        }
        LOG_ERROR(getLogger("InjectPause"), "[{}] injectPauseWait end {}", name, err_str);
    }
    return status;
}

size_t injectPauseClear(folly::StringPiece name)
{
    if (!injectPauseEnabled())
    {
        return false;
    }

    size_t num_paused = 0;
    auto ptr = pausePoints().lock();
    if (injectPauseLogEnabled())
    {
        LOG_ERROR(getLogger("InjectPause"), "[{}] injectPauseClear ", name);
    }

    if (name.empty())
    {
        for (auto & it : *ptr)
        {
            auto & baton_set = it.second;
            num_paused += wakeupBatonSet(baton_set, 0);
            baton_set.enabled = false;
            baton_set.cv.notify_all();
        }
    }
    else
    {
        auto it = ptr->find(name.str());
        if (it != ptr->end())
        {
            auto & baton_set = it->second;
            num_paused += wakeupBatonSet(baton_set, 0) > 0 ? 1 : 0;
            baton_set.enabled = false;
            baton_set.cv.notify_all();
        }
    }
    return num_paused;
}

}
