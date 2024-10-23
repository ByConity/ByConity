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

#include <CloudServices/CnchWorkerResource.h>
#include <Interpreters/Context.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>

#include <chrono>

namespace CurrentMetrics
{
extern const Metric ActiveCnchSession;
extern const Metric ActiveHttpSession;
}

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
    Poco::Timestamp current_time;
    std::unique_lock lock(mutex);

    auto & sessions_by_key = sessions.template get<1>();
    auto it = sessions_by_key.find(session_id);
    bool session_exist = true;
    if (it == sessions_by_key.end())
    {
        if (return_null_if_not_found)
            return nullptr;

        if (throw_if_not_found)
            throw Exception("Session not found.", ErrorCodes::SESSION_NOT_FOUND);

        session_exist = false;
        it = sessions_by_key.insert(std::make_shared<NamedSession>(session_id, context, timeout, *this)).first;
    }

    /// Use existing session.
    const auto & session = *it;

    /// For cnch, it's of for session to not be unique, e.g. in union query, the sub-query will have same transaction id,
    /// therefore they shared same session on worker.
    if constexpr (!std::is_same_v<NamedSession, NamedCnchSession>)
    {
        if (!session.unique())
            throw Exception("Session is locked by a concurrent client.", ErrorCodes::SESSION_IS_LOCKED);

        // If http session enter again, try update timeout of it
        if (session_exist)
        {
            size_t new_close_time = current_time.epochTime() + timeout;
            if (session->close_time < new_close_time)
                sessions.template get<1>().modify(it, [&new_close_time](auto & temp) { temp->close_time = new_close_time; });
        }
    }

    return session;
}

template <typename NamedSession>
std::vector<std::pair<typename NamedSession::NamedSessionKey, std::shared_ptr<CnchWorkerResource>>>
NamedSessionsImpl<NamedSession>::getAllWorkerResources() const
{
    std::vector<std::pair<Key, CnchWorkerResourcePtr>> res;
    std::lock_guard lock(mutex);
    for (const auto & session : sessions)
    {
        if (auto resource = session->context->getCnchWorkerResource())
            res.emplace_back(session->key, resource);
    }
    return res;
}

template<typename NamedSession>
void NamedSessionsImpl<NamedSession>::cleanThread()
{
    setThreadName("SessionCleaner");


    while (true)
    {
        auto interval = closeSessions();

        std::unique_lock lock{mutex};
        if (cond.wait_for(lock, interval, [this]() -> bool { return quit; }))
            break;
    }
}

template <typename NamedSession>
std::chrono::steady_clock::duration NamedSessionsImpl<NamedSession>::closeSessions()
{
    static constexpr std::chrono::steady_clock::duration close_interval = std::chrono::seconds(10);
    static constexpr size_t max_batch_clean_size = 100;
    Poco::Timestamp current_time;
    std::vector<String> released_sessions;

    {
        std::unique_lock lock{mutex};
        auto & sessions_by_close = sessions.template get<0>();
        auto session_iter = sessions_by_close.begin();

        while (session_iter != sessions_by_close.end()
               && (*session_iter)->close_time
                   <= static_cast<size_t>(current_time.epochTime() && released_sessions.size() < max_batch_clean_size))
        {
            if ((*session_iter).unique())
            {
                released_sessions.emplace_back((*session_iter)->getID());
                session_iter = sessions_by_close.erase(session_iter);
            }
            else
                session_iter++;
        }
    }

    for (auto & session_id : released_sessions)
        LOG_INFO(getLogger("NamedSessionImpl"), "release timed out session: {}", session_id);

    return close_interval;
}

NamedSession::NamedSession(NamedSessionKey key_, ContextPtr context_, size_t timeout_, NamedSessions & parent_)
    : key(key_), context(Context::createCopy(context_)), timeout(timeout_), parent(parent_)
{
    CurrentMetrics::add(CurrentMetrics::ActiveHttpSession);
    close_time = Poco::Timestamp().epochTime() + timeout;
}

NamedSession::~NamedSession()
{
    CurrentMetrics::sub(CurrentMetrics::ActiveHttpSession);
}

void NamedSession::release()
{
    parent.tryUpdateSessionCloseTime(*this, Poco::Timestamp().epochTime() + timeout);
}

NamedCnchSession::NamedCnchSession(NamedSessionKey key_, ContextPtr context_, size_t timeout_, NamedCnchSessions & parent_)
    : key(key_), context(Context::createCopy(context_)), timeout(timeout_), parent(parent_)
{
    CurrentMetrics::add(CurrentMetrics::ActiveCnchSession);
    context->worker_resource = std::make_shared<CnchWorkerResource>();
    close_time = Poco::Timestamp().epochTime() + timeout;
}

NamedCnchSession::~NamedCnchSession()
{
     CurrentMetrics::sub(CurrentMetrics::ActiveCnchSession);
}

void NamedCnchSession::release()
{
    parent.releaseSession(*this);
    LOG_DEBUG(getLogger("NamedCnchSession"), "release CnchWorkerResource({})", key);
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
