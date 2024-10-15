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
#include <Common/SipHash.h>
#include <Common/ThreadPool.h>
#include <Interpreters/Context_fwd.h>

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
    using SessionKeyHash = typename NamedSession::SessionKeyHash;

    ~NamedSessionsImpl();

    /// Find existing session or create a new.
    std::shared_ptr<NamedSession> acquireSession(
        const Key & session_id,
        ContextPtr context,
        size_t timeout,
        bool throw_if_not_found,
        bool return_null_if_not_found = false);

    void releaseSession(NamedSession & session)
    {
        std::unique_lock lock(mutex);
        scheduleCloseSession(session, lock);
    }

    std::vector<std::pair<Key, std::shared_ptr<CnchWorkerResource>>> getAllWorkerResources() const;

private:
    using Container = std::unordered_map<Key, std::shared_ptr<NamedSession>, SessionKeyHash>;
    using CloseTimes = std::multimap<size_t, Key>;
    Container sessions;
    CloseTimes close_times;

    void scheduleCloseSession(NamedSession & session, std::unique_lock<std::mutex> &);

    void cleanThread();

    /// Close sessions, that has been expired. Returns how long to wait for next session to be expired, if no new sessions will be added.
    std::chrono::steady_clock::duration closeSessions(std::unique_lock<std::mutex> & lock);

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
    using NamedSessionKey = std::pair<String, String>;
    NamedSessionKey key;
    ContextMutablePtr context;
    size_t timeout;
    size_t close_time{0};
    NamedSessionsImpl<NamedSession> & parent;

    NamedSession(NamedSessionKey key_, ContextPtr context_, size_t timeout_, NamedSessions & parent_);
    void release();

    String getID() const { return key.first + "-" + key.second; }

    class SessionKeyHash
    {
    public:
        size_t operator()(const NamedSessionKey & session_key) const
        {
            SipHash hash;
            hash.update(session_key.first);
            hash.update(session_key.second);
            return hash.get64();
        }
    };
};

struct NamedCnchSession
{
    using NamedSessionKey = UInt64;
    using SessionKeyHash = std::hash<NamedSessionKey>;

    NamedSessionKey key;
    ContextMutablePtr context;
    size_t timeout;
    size_t close_time{0};
    NamedSessionsImpl<NamedCnchSession> & parent;

    NamedCnchSession(NamedSessionKey key_, ContextPtr context_, size_t timeout_, NamedCnchSessions & parent_);
    void release();

    std::optional<std::atomic_size_t> plan_segments_count;
    void registerPlanSegmentsCount(size_t _plan_segments_count);
    void eliminateCurrentPlanSegment();

    String getID() const { return std::to_string(key); }
};

}
