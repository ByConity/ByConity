/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Interpreters/NamedSession.h>

#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Transaction/TxnTimestamp.h>
#include <CloudServices/CnchWorkerResource.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int SESSION_NOT_FOUND;
    extern const int SESSION_IS_LOCKED;
}

template<typename NamedSession>
NamedSessionsImpl<NamedSession>::~NamedSessionsImpl()
{
    try
    {
        {
            std::lock_guard lock{mutex};
            quit = true;
        }

        cond.notify_one();
        thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

template<typename NamedSession>
std::shared_ptr<NamedSession> NamedSessionsImpl<NamedSession>::acquireSession(
    const Key & session_id,
    ContextPtr context,
    size_t timeout,
    bool throw_if_not_found,
    bool return_null_if_not_found)
{
    std::unique_lock lock(mutex);

    auto it = sessions.find(session_id);
    if (it == sessions.end())
    {
        if (return_null_if_not_found)
        {
            return nullptr;
        }

        if (throw_if_not_found)
            throw Exception("Session not found.", ErrorCodes::SESSION_NOT_FOUND);
        it = sessions.insert(std::make_pair(session_id, std::make_shared<NamedSession>(session_id, context, timeout, *this))).first;
    }

    /// Use existing session.
    const auto & session = it->second;

    scheduleCloseSession(*session, lock);

    /// For cnch, it's of for session to not be unique, e.g. in union query, the sub-query will have same transaction id,
    /// therefore they shared same session on worker.
    if constexpr (!std::is_same_v<NamedSession, NamedCnchSession>)
    {
        if (!session.unique())
            throw Exception("Session is locked by a concurrent client.", ErrorCodes::SESSION_IS_LOCKED);
    }

    return session;
}

template<typename NamedSession>
std::vector<std::pair<typename NamedSession::NamedSessionKey, std::shared_ptr<CnchWorkerResource>>> NamedSessionsImpl<NamedSession>::getAllWorkerResources() const
{
    std::lock_guard lock(mutex);
    std::vector<std::pair<Key, CnchWorkerResourcePtr>> res;
    for (const auto & [key, session]: sessions)
    {
        if (auto resource = session->context->getCnchWorkerResource())
            res.emplace_back(key, resource);
    }
    return res;
}

template<typename NamedSession>
void NamedSessionsImpl<NamedSession>::scheduleCloseSession(NamedSession & session, std::unique_lock<std::mutex> &)
{
    /// Push it on a queue of sessions to close, on a position corresponding to the timeout.
    /// (timeout is measured from current moment of time)

    Poco::Timestamp current_time;
    if (session.close_time < current_time.epochTime() + session.timeout)
    {
        session.close_time = current_time.epochTime() + session.timeout;
        close_times.emplace(session.close_time, session.key);
    }
}

template<typename NamedSession>
void NamedSessionsImpl<NamedSession>::cleanThread()
{
    setThreadName("SessionCleaner");
    std::unique_lock lock{mutex};

    while (true)
    {
        auto interval = closeSessions(lock);

        if (cond.wait_for(lock, interval, [this]() -> bool { return quit; }))
            break;
    }
}

template<typename NamedSession>
std::chrono::steady_clock::duration NamedSessionsImpl<NamedSession>::closeSessions(std::unique_lock<std::mutex> & lock)
{
    /// Schedule closeSessions() every 1 second by default.
    static constexpr std::chrono::steady_clock::duration close_interval = std::chrono::seconds(1);
    auto log = getLogger("NamedSession");

    if (close_times.empty())
        return close_interval;

    Poco::Timestamp current_time;

    while (!close_times.empty())
    {
        auto curr_session = *close_times.begin();

        if (curr_session.first > static_cast<size_t>(current_time.epochTime()))
            break;

        auto session_iter = sessions.find(curr_session.second);

        if (session_iter != sessions.end() && session_iter->second->close_time == curr_session.first)
        {
            if (!session_iter->second.unique())
            {
                /// Skip to recycle and move it to close on the next interval.
                session_iter->second->timeout = 1;
                scheduleCloseSession(*session_iter->second, lock);
            }
            else
            {
                LOG_DEBUG(log, "Release timed out session: {}", session_iter->second->getID());
                sessions.erase(session_iter);
            }
        }

        close_times.erase(close_times.begin());
    }

    return close_interval;
}


NamedSession::NamedSession(NamedSessionKey key_, ContextPtr context_, size_t timeout_, NamedSessions & parent_)
    : key(key_), context(Context::createCopy(context_)), timeout(timeout_), parent(parent_)
{
}

void NamedSession::release()
{
    parent.releaseSession(*this);
}

NamedCnchSession::NamedCnchSession(NamedSessionKey key_, ContextPtr context_, size_t timeout_, NamedCnchSessions & parent_)
    : key(key_), context(Context::createCopy(context_)), timeout(timeout_), parent(parent_)
{
    context->worker_resource = std::make_shared<CnchWorkerResource>();
}

void NamedCnchSession::release()
{
    timeout = 0; /// schedule immediately
    close_time = 0;
    parent.releaseSession(*this);
    LOG_DEBUG(getLogger("NamedCnchSession"), "Release CnchWorkerResource {}", key);
}

void NamedCnchSession::registerPlanSegmentsCount(size_t _plan_segments_count)
{
    plan_segments_count = _plan_segments_count;
}

void NamedCnchSession::eliminateCurrentPlanSegment()
{
    if (plan_segments_count && --(*plan_segments_count) == 0)
        release();
}

template class NamedSessionsImpl<NamedSession>;
template class NamedSessionsImpl<NamedCnchSession>;

}
