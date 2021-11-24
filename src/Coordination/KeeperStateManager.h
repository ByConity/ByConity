#pragma once

#include <Core/Types.h>
#include <string>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/CoordinationSettings.h>
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

using KeeperServerConfigPtr = nuraft::ptr<nuraft::srv_config>;

/// When our configuration changes the following action types
/// can happen
enum class ConfigUpdateActionType
{
    RemoveServer,
    AddServer,
    UpdatePriority,
};

/// Action to update configuration
struct ConfigUpdateAction
{
    ConfigUpdateActionType action_type;
    KeeperServerConfigPtr server;
};

using ConfigUpdateActions = std::vector<ConfigUpdateAction>;

/// Responsible for managing our and cluster configuration
class KeeperStateManager : public nuraft::state_mgr
{
public:
    KeeperStateManager(
        int server_id_,
        const std::string & config_prefix,
        const std::string & log_storage_path,
        const Poco::Util::AbstractConfiguration & config,
        const CoordinationSettingsPtr & coordination_settings);

    KeeperStateManager(
        int server_id_,
        const std::string & host,
        int port,
        const std::string & logs_path);

    void loadLogStore(uint64_t last_commited_index, uint64_t logs_to_keep);

    void flushLogStore();

    nuraft::ptr<nuraft::cluster_config> load_config() override { return cluster_config; }

    void save_config(const nuraft::cluster_config & config) override;

    void save_state(const nuraft::srv_state & state) override;

    nuraft::ptr<nuraft::srv_state> read_state() override { return server_state; }

    nuraft::ptr<nuraft::log_store> load_log_store() override { return log_store; }

    Int32 server_id() override { return my_server_id; }

    nuraft::ptr<nuraft::srv_config> get_srv_config() const { return my_server_config; }

    void system_exit(const int /* exit_code */) override {}

    int getPort() const { return my_port; }

    bool shouldStartAsFollower() const
    {
        return start_as_follower_servers.count(my_server_id);
    }

    bool isSecure() const
    {
        return secure;
    }

    nuraft::ptr<KeeperLogStore> getLogStore() const { return log_store; }

    uint64_t getTotalServers() const { return total_servers; }

private:
    /// Wrapper struct for Keeper cluster config. We parse this
    /// info from XML files.
    struct KeeperConfigurationWrapper
    {
        /// Our port
        int port;
        /// Our config
        KeeperServerConfigPtr config;
        /// Servers id's to start as followers
        std::unordered_set<int> servers_start_as_followers;
        /// Cluster config
        // ClusterConfigPtr cluster_config;
    };

    int my_server_id;
    int my_port;
    bool secure;
    uint64_t total_servers{0};
    std::unordered_set<int> start_as_follower_servers;
    nuraft::ptr<KeeperLogStore> log_store;
    nuraft::ptr<nuraft::srv_config> my_server_config;
    nuraft::ptr<nuraft::cluster_config> cluster_config;
    nuraft::ptr<nuraft::srv_state> server_state;
};

}
