/*
 * Copyright (2023) Bytedance Ltd. and/or its affiliates
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

#include <Common/StorageElection/StorageElector.h>

#include <atomic>
#include <exception>
#include <optional>
#include <Protos/RPCHelpers.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>

namespace DB
{

// Prepare for the leader election.
StorageElector::StorageElector(
    IKvStoragePtr store_,
    uint64_t refresh_interval_ms,
    uint64_t expired_interval_ms,
    const HostWithPorts & host_info,
    const String & election_key_,
    LeadershipHandler on_leader_,
    LeadershipHandler on_follower_)
    : store(std::move(store_))
    , election_key(election_key_)
    , on_leader(std::move(on_leader_))
    , on_follower(std::move(on_follower_))
    , logger(getLogger("StorageElector"))
{
    local_info.refresh_interval_ms = refresh_interval_ms;
    local_info.expired_interval_ms = expired_interval_ms;

    RPCHelpers::fillHostWithPorts(host_info, local_address);

    start();
}

StorageElector::~StorageElector()
{
    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

static uint64_t getCurrentTimeMs()
{
    struct timeval tm;
    gettimeofday(&tm, nullptr);
    return tm.tv_sec * 1000 + tm.tv_usec / 1000;
}

String StorageElector::toString(Role role)
{
    switch (role)
    {
        case Role::Follower:
            return "Follower";
        case Role::Leader:
            return "Leader";
    }
    __builtin_unreachable();
};

bool StorageElector::setRole(Role role_)
{
    if (role.load(std::memory_order::acquire) != role_)
    {
        auto old_role = role.load(std::memory_order::acquire);
        if (role_ == Role::Leader)
        {
            role.store(role_, std::memory_order::release);
            bool ret = false;
            try
            {
                ret = on_leader(curr_leader_host.has_value() ? &curr_leader_host.value() : nullptr);
            }
            catch (...)
            {
                //We do nothing here because ret==false.
            }
            if (!ret)
            {
                LOG_ERROR(
                    logger,
                    "Failed to call on_leader, try to yield leader on {}",
                    curr_leader_host.has_value() ? curr_leader_host->toDebugString() : "nullptr");

                doYield(false);
                return false;
            }
        }
        else if (role_ == Role::Follower)
        {
            role.store(role_, std::memory_order::release);
            on_follower(curr_leader_host.has_value() ? &curr_leader_host.value() : nullptr);
        }
        else
        {
            role.store(role_, std::memory_order::release);
        }

        LOG_INFO(logger, "Change role {} -> {}", toString(old_role), toString(role_));
    }
    return true;
}

Protos::LeaderInfo StorageElector::generateInitInfo()
{
    Protos::LeaderInfo info;
    auto current_time = getCurrentTimeMs();

    auto & address = *info.mutable_address();
    address.CopyFrom(local_address);

    auto & lease = *info.mutable_lease();
    lease.set_elected_time(current_time);
    lease.set_last_refresh_time(current_time);
    lease.set_refresh_interval_ms(local_info.refresh_interval_ms);
    lease.set_expired_interval_ms(local_info.expired_interval_ms);
    lease.set_status(Protos::LeaderLease::Ready);

    info.mutable_previous_leader_address()->CopyFrom(local_address);
    info.mutable_previous_leader_lease()->CopyFrom(lease);
    return info;
}

bool StorageElector::isSameLeader(const Protos::LeaderInfo & info)
{
    return local_address.rpc_port() == info.address().rpc_port() && local_address.tcp_port() == info.address().tcp_port()
        && local_address.http_port() == info.address().http_port() && local_address.hostname() == info.address().hostname()
        && local_address.host() == info.address().host();
}

bool StorageElector::isLeaderExpired()
{
    if (role.load(std::memory_order::acquire) != Role::Leader)
        return true;
    auto now = getCurrentTimeMs();
    auto expired_interval_ms = local_info.expired_interval_ms;
    return now >= last_refresh_local_time + expired_interval_ms;
}

bool StorageElector::isLeader()
{
    return !isLeaderExpired();
}

void StorageElector::yieldLeadership()
{
    stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(local_info.expired_interval_ms));
    start();
}

bool StorageElector::isLeaseExpired(const Protos::LeaderInfo & remote_info)
{
    auto now = getCurrentTimeMs();
    auto expired_interval_ms = remote_info.lease().expired_interval_ms();
    auto local_expired_interval_ms = last_leader_info.lease().expired_interval_ms();

    return (remote_info.lease().status() == Protos::LeaderLease::Yield)
        || (now >= remote_info.lease().last_refresh_time() + expired_interval_ms
            && now >= last_refresh_local_time + local_expired_interval_ms);
}

// Leader will record the refresh time before it send the refresh request;
// Follower will record the refresh time after it receive the refresh request;
// So when followers consider the leader lease is expired, leader should consider it is expired already.
bool StorageElector::tryUpdateLocalRecord(const Protos::LeaderInfo & remote_info, const String & remote_info_string, bool try_become_leader)
{
    bool expired = isLeaseExpired(remote_info);
    if (remote_info.lease().last_refresh_time() == last_leader_info.lease().last_refresh_time())
        return expired;

    Role new_role = Role::Follower;
    if (expired)
    {
        // Want to update remote record, but expired.
        if (try_become_leader)
            last_refresh_local_time = remote_info.lease().last_refresh_time();
        // Receive a expired remote record
        else
            last_refresh_local_time = getCurrentTimeMs();

        std::lock_guard lock(leader_host_mutex);
        curr_leader_host.reset();
    }
    else
    {
        if (try_become_leader)
        {
            last_refresh_local_time = remote_info.lease().last_refresh_time();
            new_role = Role::Leader;
        }
        else
        {
            last_refresh_local_time = getCurrentTimeMs();
        }

        auto new_leader = RPCHelpers::createHostWithPorts(remote_info.address());
        std::lock_guard lock(leader_host_mutex);
        curr_leader_host = new_leader;
    }

    last_leader_info.CopyFrom(remote_info);
    last_leader_info_string = remote_info_string;

    setRole(new_role);
    if (new_role == Role::Leader)
    {
        expired = isLeaseExpired(remote_info);
    }

    return expired;
}

void StorageElector::tryUpdateRemoteRecord(bool refreshed, bool yield)
{
    Protos::LeaderInfo new_leader_info;
    if (!refreshed)
    {
        new_leader_info.mutable_previous_leader_address()->CopyFrom(last_leader_info.address());
        new_leader_info.mutable_previous_leader_lease()->CopyFrom(last_leader_info.lease());
    }
    else
    {
        new_leader_info.mutable_previous_leader_address()->CopyFrom(last_leader_info.previous_leader_address());
        new_leader_info.mutable_previous_leader_lease()->CopyFrom(last_leader_info.previous_leader_lease());
    }

    new_leader_info.mutable_address()->CopyFrom(local_address);

    auto now = getCurrentTimeMs();
    auto * lease = new_leader_info.mutable_lease();

    if (!refreshed)
        lease->set_elected_time(now);
    else
        lease->set_elected_time(last_leader_info.lease().elected_time());
    lease->set_last_refresh_time(now);
    lease->set_refresh_interval_ms(local_info.refresh_interval_ms);
    lease->set_expired_interval_ms(local_info.expired_interval_ms);
    if (yield)
        lease->set_status(Protos::LeaderLease::Yield);
    else
        lease->set_status(Protos::LeaderLease::Ready);

    String leader_info_string;
    new_leader_info.SerializeToString(&leader_info_string);

    if (!refreshed && !yield)
        LOG_INFO(logger, "Try to become new leader, My info: {}", new_leader_info.DebugString());

    if (yield)
        LOG_INFO(logger, "Try to yield leader, My info: {}", new_leader_info.DebugString());

    auto result = store->putCAS(election_key, leader_info_string, last_leader_info_string, true);

    if (!yield)
    {
        auto now_after_put = getCurrentTimeMs();
        auto local_expired_interval_ms = last_leader_info.lease().expired_interval_ms();
        if (now_after_put >= last_refresh_local_time + local_expired_interval_ms)
            LOG_WARNING(logger, "Elect or refresh operation cost too much time, now: {}, last_refresh_local_time: {}", now_after_put, last_refresh_local_time);
    }
    
    if (result.first)
    {
        if (tryUpdateLocalRecord(new_leader_info, leader_info_string, !yield))
        {
            LOG_WARNING(
                logger,
                "It is updated successfully on remote but expired on local: time cost : {}ms, {} ",
                getCurrentTimeMs() - now,
                new_leader_info.DebugString());
            if (!yield)
            {
                doYield(true);
            }
        }
            
        else if (!refreshed)
            LOG_INFO(
                logger,
                "It is updated successfully and not expired: time cost : {}ms, {}",
                getCurrentTimeMs() - now,
                new_leader_info.DebugString());
        // else
            // LOG_TRACE(logger, "It is refreshed successfully and not expired: {}", new_leader_info.DebugString());
    }
    else
    {
        LOG_INFO(logger, "Maybe others have become the new leader!");
        Protos::LeaderInfo latest_leader_info;
        if (!latest_leader_info.ParseFromString(result.second))
        {
            LOG_ERROR(logger, "Unknown format for election storage, content in kv");
            return;
        }

        // Do not check whether it is an inflight request sent from current node to avoid infinite recursion.
        if (!tryUpdateLocalRecord(latest_leader_info, result.second, false))
            LOG_INFO(logger, "New leader info: {}", latest_leader_info.DebugString());
        else
            LOG_INFO(logger, "New leader expired info: {}", latest_leader_info.DebugString());
    }
}

bool StorageElector::initKey()
{
    doFollowerCheck();
    if (!initted)
        LOG_ERROR(logger, "StorageElector init failed");

    return initted;
}

void StorageElector::doLeaderCheck()
{
    if (role.load(std::memory_order::acquire) != Role::Leader)
    {
        sleep_time = std::chrono::milliseconds::zero();
        return;
    }
    sleep_time = std::chrono::milliseconds(local_info.probe_interval_ms);
    try
    {
        if (isLeaseExpired(last_leader_info))
        {
            LOG_INFO(logger, "Current node maybe to busy to refresh the lease. Yield first. last_leader_info: {}", last_leader_info.DebugString());
            doYield(true);
        }
        else
            tryUpdateRemoteRecord(true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageElector::doFollowerCheck()
{
    if (role.load(std::memory_order::acquire) != Role::Follower)
    {
        sleep_time = std::chrono::milliseconds::zero();
        return;
    }
    sleep_time = std::chrono::milliseconds(local_info.probe_interval_ms);
    try
    {
        String result;
        auto version = store->get(election_key, result);

        if (result.empty() && version == 0)
        {
            if (initted)
            {
                LOG_FATAL(logger, "We lost the election key result unexpectedly!");
                std::terminate();
            }

            Protos::LeaderInfo leader_info = generateInitInfo();
            String leader_info_string;
            leader_info.SerializeToString(&leader_info_string);
            store->put(election_key, leader_info_string, true);
            if (!tryUpdateLocalRecord(leader_info, leader_info_string, true))
            {
                LOG_INFO(logger, "Init the election key successfully, current node is the leader.");
            }
            else
            {
                LOG_INFO(logger, "Init the election key successfully, but the lease expired.");
            }
            initted = true;
        }
        else if (!result.empty() && version != 0)
        {
            Protos::LeaderInfo leader_info;
            if (!leader_info.ParseFromString(result))
            {
                LOG_ERROR(logger, "Unknown format from remote election storage, content: {}", result);
                sleep_time = std::chrono::milliseconds(local_info.probe_interval_ms);
                return;
            }

            // An in-flight put request makes current node to become a leader already.
            if (isSameLeader(leader_info))
            {
                LOG_INFO(
                    logger,
                    "Current node used to be a leader. May be an in-flight put request "
                    "or it yiled the leader role for a long time but other nodes did not be the leader. "
                    "Old leader info: {}",
                    leader_info.DebugString());

                bool expired = tryUpdateLocalRecord(leader_info, result, false);
                // Refresh the lease or try to be the leader
                if (!expired && leader_info.lease().status() != Protos::LeaderLease::Yield)
                    tryUpdateRemoteRecord(true);
                else
                {
                    tryUpdateRemoteRecord(false);
                    if (role.load(std::memory_order::acquire) != Role::Follower)
                        sleep_time = std::chrono::milliseconds::zero();
                }
            }
            else
            {
                bool expired = tryUpdateLocalRecord(leader_info, result, false);
                // Try to be the leader
                if (expired)
                {
                    tryUpdateRemoteRecord(false);
                    if (role.load(std::memory_order::acquire) != Role::Follower)
                        sleep_time = std::chrono::milliseconds::zero();
                }
            }

            initted = true;
        }
        else
        {
            LOG_ERROR(logger, "Unexpected kv result!");
        }
    }
    catch (...)
    {
        /// TODO: what should we do?
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageElector::doYield(bool soft)
{
    // Even cas failed, the node should be switched to follower first.
    if (role != Role::Leader)
        return;
    {
        std::lock_guard lock(leader_host_mutex);
        curr_leader_host.reset();
    }

    setRole(Role::Follower);
     // You can not try to become a leader immediately. Wait for a longer time.
    sleep_time = std::chrono::milliseconds(2 * local_info.expired_interval_ms);

    if (soft)
        return;

    try
    {
        tryUpdateRemoteRecord(true, true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageElector::start()
{
    LOG_INFO(logger, "election_key: {}", election_key);
    {
        std::unique_lock<std::mutex> lk(cv_m);
        if (!thread_stop)
        {
            LOG_ERROR(logger, "You should not start an elector again!");
            return;
        }
        thread_stop = false;
    }

    bg_thread = ThreadFromGlobalPool([this] {
        setThreadName("LeaderChecker");
        do
        {
            doFollowerCheck();
            if (sleep_time != std::chrono::milliseconds::zero())
            {
                std::unique_lock<std::mutex> lk(cv_m);
                if (cv.wait_for(lk, sleep_time, [this]() { return thread_stop; }))
                    break;
            }

            doLeaderCheck();
            if (sleep_time != std::chrono::milliseconds::zero())
            {
                std::unique_lock<std::mutex> lk(cv_m);
                if (cv.wait_for(lk, sleep_time, [this]() { return thread_stop; }))
                    break;
            }
        } while (true);
        doYield(false);
        LOG_INFO(logger, "StorageElector thread is now stopped!");
    });
}

void StorageElector::stop()
{
    bool need_join = false;
    {
        std::unique_lock<std::mutex> lk(cv_m);
        if (!thread_stop)
        {
            thread_stop = true;
            need_join = true;
        }
    }

    if (need_join && bg_thread.joinable())
        bg_thread.join();
}

std::optional<HostWithPorts> StorageElector::getLeaderInfo() const
{
    std::lock_guard lock(leader_host_mutex);
    return curr_leader_host;
}

}
