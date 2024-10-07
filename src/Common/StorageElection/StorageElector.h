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

#pragma once

#include <Common/Logger.h>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <optional>
#include <thread>
#include <stdint.h>
#include <Protos/cnch_common.pb.h>
#include <boost/noncopyable.hpp>
#include <Common/HostWithPorts.h>
#include <Common/ThreadPool.h>
#include <common/types.h>
#include "KvStorage.h"


namespace DB
{
using HostWithPortsPtr = std::shared_ptr<HostWithPorts>;


class StorageElector : public boost::noncopyable
{
public:
    using LeadershipHandler = std::function<bool(const HostWithPorts *)>;

    // Prepare for the leader election.
    StorageElector(
        IKvStoragePtr store_,
        uint64_t refresh_interval_ms,
        uint64_t expired_interval_ms,
        const HostWithPorts & host_info,
        const String & election_key_,
        LeadershipHandler on_leader_,
        LeadershipHandler on_follower_);

    ~StorageElector();

    void yieldLeadership();
    bool isLeader();
    void stop();

    std::optional<HostWithPorts> getLeaderInfo() const;

private:
    // Should not be changed after registered.
    IKvStoragePtr store;
    struct LeaseInfo
    {
        union
        {
            uint64_t probe_interval_ms;
            uint64_t refresh_interval_ms;
        };
        uint64_t expired_interval_ms;
    } local_info;
    Protos::HostWithPorts local_address;
    String election_key;
    LeadershipHandler on_leader;
    LeadershipHandler on_follower;

    bool initted = false;
    enum class Role
    {
        Follower = 0,
        Leader = 1
    };

    static String toString(Role role);

    std::atomic<Role> role{Role::Follower};
    Protos::LeaderInfo last_leader_info;
    String last_leader_info_string;

    mutable std::mutex leader_host_mutex;
    std::optional<HostWithPorts> curr_leader_host{std::nullopt};

    ThreadFromGlobalPool bg_thread;
    bool thread_stop{true};
    mutable std::condition_variable cv;
    mutable std::mutex cv_m;

    std::chrono::milliseconds sleep_time{std::chrono::milliseconds::zero()};
    uint64_t last_refresh_local_time{0};

    LoggerPtr logger = nullptr;

    void start();

    bool initKey();
    Protos::LeaderInfo generateInitInfo();

    bool isSameLeader(const Protos::LeaderInfo & info);
    bool isLeaseExpired(const Protos::LeaderInfo & info);
    bool isLeaderExpired();

    // Return wait time for another round of check.
    void doLeaderCheck();
    void doFollowerCheck();
    void doYield(bool soft);

    bool setRole(Role role_);
    void tryUpdateRemoteRecord(bool refreshed, bool yield = false);
    bool tryUpdateLocalRecord(const Protos::LeaderInfo & remote_info, const String & remote_info_string, bool try_become_leader);
};

}
