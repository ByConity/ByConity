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

#include <Common/Logger.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/StorageElection/StorageElector.h>
#include <daemon/BaseDaemon.h>
#include <Server/IServer.h>
#include <TSO/TSOProxy.h>
#include <TSO/Defines.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Timer.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Interpreters/Context_fwd.h>
#include <Server/HTTP/HTTPServer.h>


#define TSO_VERSION "1.0.0"

namespace DB
{

class KeeperDispatcher;
class ProtocolServerAdapter;
using ProtocolServerAdapterPtr = std::shared_ptr<ProtocolServerAdapter>;

namespace TSO
{

class TSOImpl;

class TSOServer : public BaseDaemon, public IServer
{

public:
    using ServerApplication::run;
    using TSOProxyPtr = std::shared_ptr<TSOProxy>;
    using TSOServicePtr = std::shared_ptr<TSOImpl>;

    TSOServer();
    ~TSOServer() override;

    void defineOptions(Poco::Util::OptionSet & _options) override;

    void initialize(Poco::Util::Application &) override;

    void syncTSO();

    void updateTSO();

    String getHostPort() const { return host_port; }

    Poco::Util::LayeredConfiguration & config() const override
    {
        return BaseDaemon::config();
    }

    Poco::Logger & logger() const override
    {
        return BaseDaemon::logger();
    }

    ContextMutablePtr context() const override
    {
        return global_context;
    }

    bool isCancelled() const override
    {
        return BaseDaemon::isCancelled();
    }

    /// Functions for exposing metrics
    int getNumYieldedLeadership() const { return num_yielded_leadership; }
    String tryGetTSOLeaderHostPort() const;
    UInt64 getNumStopUpdateTsFromTSOService() const;

    bool isLeader() const;

protected:
    int run() override;

    int main(const std::vector<std::string> & args) override;

private:
    friend class TSOImpl;

    LoggerPtr log;

    size_t tso_window;

    String host_port;

    TSOProxyPtr proxy_ptr;
    TSOServicePtr tso_service;

    UInt64 t_next;  /// TSO physical time
    UInt64 t_last;  /// TSO physical time upper bound (persist in KV)

    BackgroundSchedulePool::TaskHolder update_tso_task;

    ContextMutablePtr global_context;

    std::unique_ptr<StorageElector> leader_election;

    /// keep tcp servers for clickhouse-keeper
    std::vector<ProtocolServerAdapterPtr> keeper_servers;

    // Metrics
    int num_yielded_leadership;

    bool onLeader();
    bool onFollower();
    void initLeaderElection();

    Poco::Net::SocketAddress socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure = false) const;
};

}

}
