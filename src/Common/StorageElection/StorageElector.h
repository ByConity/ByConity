#pragma once

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

    bool isLeader();

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

    Poco::Logger * logger;

    void start();
    void stop();

    bool initKey();
    Protos::LeaderInfo generateInitInfo();

    bool isSameLeader(const Protos::LeaderInfo & info);
    bool isLeaseExpired(const Protos::LeaderInfo & info);
    bool isLeaderExpired();

    // Return wait time for another round of check.
    void doLeaderCheck();
    void doFollowerCheck();
    void doYield();

    bool setRole(Role role_);
    void tryUpdateRemoteRecord(bool refreshed, bool yield = false);
    bool tryUpdateLocalRecord(const Protos::LeaderInfo & remote_info, const String & remote_info_string, bool try_become_leader);
};

}
