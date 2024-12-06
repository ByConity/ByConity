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

#pragma once
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <Common/SipHash.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <mutex>


namespace DB
{

class CnchWorkerResource;

template<typename NamedSession>
class NamedSessionsImpl
{
public:
    using Key = typename NamedSession::NamedSessionKey;
    using SessionPtr = std::shared_ptr<NamedSession>;
    // sessions can be indexed by both close_time or txn_id
    // we sort sessions by close_time to release timeout session in order, use txd_id to release specific session
    using SessionContainer = boost::multi_index::multi_index_container<
        SessionPtr,
        boost::multi_index::indexed_by<
            boost::multi_index::ordered_non_unique<boost::multi_index::member<NamedSession, size_t, &NamedSession::close_time>>,
            boost::multi_index::hashed_unique<boost::multi_index::member<NamedSession, Key, &NamedSession::key>>>>;

    ~NamedSessionsImpl();

    /// Find existing session or create a new.
    std::shared_ptr<NamedSession> acquireSession(
        const Key & session_id, ContextPtr context, size_t timeout, bool throw_if_not_found, bool return_null_if_not_found = false);

    void releaseSession(NamedSession & session)
    {
        LOG_DEBUG(getLogger("NamedSessionImpl"), "release finished session: {}", session.getID());
        std::unique_lock lock(mutex);
        auto & sessions_by_key = sessions.template get<1>();
        sessions_by_key.erase(session.key);
    }

    void tryUpdateSessionCloseTime(NamedSession & session, size_t new_close_time)
    {
        if (session.close_time < new_close_time)
        {
            std::unique_lock lock(mutex);
            auto & sessions_by_key = sessions.template get<1>();
            sessions_by_key.modify(
                sessions_by_key.find(session.key), [&new_close_time](auto & temp) { temp->close_time = new_close_time; });
        }
    }

    std::vector<std::pair<Key, std::shared_ptr<CnchWorkerResource>>> getAllWorkerResources() const;

    // Used only for test
    size_t getCurrentActiveSession() const
    {
        std::unique_lock lock(mutex);
        return sessions.size();
    }


private:
    SessionContainer sessions;

    void cleanThread();

    /// Close sessions, that has been expired. ATTENTION: you need have a lock before calling this method.
    std::chrono::steady_clock::duration closeSessions();

    mutable std::mutex mutex;
    std::condition_variable cond;
    std::atomic<bool> quit{false};
    ThreadFromGlobalPool thread{&NamedSessionsImpl::cleanThread, this};
};

struct NamedSession;
struct NamedCnchSession;

using NamedSessions = NamedSessionsImpl<NamedSession>;
using NamedCnchSessions = NamedSessionsImpl<NamedCnchSession>;

/// Named sessions. The user could specify session identifier to reuse settings and temporary tables in subsequent requests.
struct NamedSession
{
    /// User name and session identifier. Named sessions are local to users.
    struct NamedSessionKey
    {
        String session_id;
        String user;

        bool operator==(const NamedSessionKey & other) const { return session_id == other.session_id && user == other.user; }
    };

    NamedSessionKey key;
    ContextMutablePtr context;
    size_t timeout{0};
    size_t close_time{0};
    NamedSessionsImpl<NamedSession> & parent;

    NamedSession(NamedSessionKey key_, ContextPtr context_, size_t timeout_, NamedSessions & parent_);
    ~NamedSession();
    void release();

    String getID() const { return key.session_id + "-" + key.user; }
};

struct NamedCnchSession
{
    using NamedSessionKey = UInt64;
    using SessionKeyHash = std::hash<NamedSessionKey>;

    NamedSessionKey key;
    ContextMutablePtr context;
    size_t timeout{0};
    size_t close_time{0};
    NamedSessionsImpl<NamedCnchSession> & parent;

    NamedCnchSession(NamedSessionKey key_, ContextPtr context_, size_t timeout_, NamedCnchSessions & parent_);
     ~NamedCnchSession();
    void release();

    std::optional<std::atomic_size_t> plan_segments_count;
    void registerPlanSegmentsCount(size_t _plan_segments_count);
    void eliminateCurrentPlanSegment();

    String getID() const { return std::to_string(key); }
};

inline std::size_t hash_value(const NamedSession::NamedSessionKey & session_key)
{
    SipHash hash;
    hash.update(session_key.session_id);
    hash.update(session_key.user);
    return hash.get64();
}

}
