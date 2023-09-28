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

#include <TSO/TSOServer.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/Net/TCPServerParams.h>
#include <Poco/Util/HelpFormatter.h>
#include <brpc/server.h>
#include "Common/Config/ConfigProcessor.h"
#include <Common/Exception.h>
#include <common/LocalDateTime.h>
#include <common/logger_useful.h>
#include <gflags/gflags.h>
#include <chrono>
#include <memory>
#include <thread>
#include <ServiceDiscovery/ServiceDiscoveryFactory.h>
#include <ServiceDiscovery/registerServiceDiscovery.h>
#include <boost/exception/diagnostic_information.hpp>
#include <common/ErrorHandlers.h>

#include <Core/Defines.h>
#include <Common/Configurations.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Coordination/Defines.h>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/FourLetterCommand.h>
#include <Server/ProtocolServerAdapter.h>
#include <Poco/Net/NetException.h>
#include <Server/KeeperTCPHandlerFactory.h>
#include <TSO/TSOImpl.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/PrometheusRequestHandler.h>
#include <Server/TSOPrometheusMetricsWriter.h>

using namespace std::chrono;

namespace brpc::policy
{
    DECLARE_string(consul_agent_addr);
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TSO_INTERNAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int TSO_OPERATION_ERROR;
}

namespace
{
int waitServersToFinish(std::vector<DB::ProtocolServerAdapterPtr> & servers, size_t seconds_to_wait)
{
    const int sleep_max_ms = 1000 * seconds_to_wait;
    const int sleep_one_ms = 100;
    int sleep_current_ms = 0;
    int current_connections = 0;

    while (sleep_current_ms < sleep_max_ms)
    {
        current_connections = 0;

        for (auto & server : servers)
        {
            server->stop();
            current_connections += server->currentConnections();
        }

        if (!current_connections)
            break;

        sleep_current_ms += sleep_one_ms;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_one_ms));
    }
    return current_connections;
}

[[noreturn]] void forceShutdown()
{
#if defined(THREAD_SANITIZER) && defined(OS_LINUX)
    /// Thread sanitizer tries to do something on exit that we don't need if we want to exit immediately,
    /// while connection handling threads are still run.
    (void)syscall(SYS_exit_group, 0);
    __builtin_unreachable();
#else
    _exit(0);
#endif
}
}

namespace TSO
{

TSOServer::TSOServer()
    : LeaderElectionBase(config().getInt64("tso_service.election_check_ms", 100))
    , timer(0, TSO_UPDATE_INTERVAL)
    , callback(*this, &TSOServer::updateTSO)
    , num_yielded_leadership(0)
{
}

TSOServer::~TSOServer() = default;

void TSOServer::defineOptions(Poco::Util::OptionSet &_options)
{
    Application::defineOptions(_options);

    _options.addOption(
        Poco::Util::Option("help", "h", "show help and exit")
            .required(false)
            .repeatable(false)
            .binding("help"));

    _options.addOption(
        Poco::Util::Option("config-file", "C", "set config file path")
            .required(false)
            .repeatable(false)
            .argument("config-file", true)
            .binding("config-file"));

}

void TSOServer::initialize(Poco::Util::Application & self)
{
    BaseDaemon::initialize(self);

    log = &logger();

    registerServiceDiscovery();

    const char * consul_http_host = getenv("CONSUL_HTTP_HOST");
    const char * consul_http_port = getenv("CONSUL_HTTP_PORT");
    if (consul_http_host != nullptr && consul_http_port != nullptr)
        brpc::policy::FLAGS_consul_agent_addr = "http://" + createHostPortString(consul_http_host, consul_http_port);

    tso_window = config().getInt("tso_service.tso_window_ms", 3000);  /// 3 seconds
    tso_max_retry_count = config().getInt("tso_service.tso_max_retry_count", 3); // TSOV: see if can keep or remove
}

void TSOServer::syncTSO()
{
    try
    {
        /// get current unix timestamp
        milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        UInt64 t_now = ms.count();
        t_next = t_now;

        /// get timestamp from KV
        UInt64 t_last_prev = proxy_ptr->getTimestamp();

        if (t_last_prev == 0)
        {
            if (t_next == 0)
            {
                t_next = 1;  /// avoid MVCC version is zero
            }
            t_last = t_next + tso_window;
        }
        else
        {
            if (t_now < t_last_prev + 1)  /// unix timestamp is in TSO window, then update t_next to t_last
            {
                t_next = t_last_prev + 1;
            }
            t_last = t_next + tso_window;
        }

        /// save to KV
        proxy_ptr->setTimestamp(t_last);
        /// sync to tso service
        tso_service->setPhysicalTime(t_next);
        LOG_TRACE(log, "This node ({}) has called syncTSO. Current status: [t_now = {}], [t_next = {}], [t_last = {}],", host_port, t_now, t_next, t_last);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw; /// TODO: call exitLeaderElection?
    }
}

void TSOServer::updateTSO(Poco::Timer &)
{
    try
    {
        /// get current unix timestamp
        milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        UInt64 t_now = ms.count();
        TSOClock cur_ts = tso_service->getClock();


        if (t_now > t_next + 1)  /// machine time is larger than physical time, keep physical time close to machine time
        {
            t_next = t_now;
        }
        else if (cur_ts.logical > MAX_LOGICAL / 2)  /// logical time buffer has been used more than half, increase physical time and clear logical time
        {
            t_next = cur_ts.physical + 1;
            LOG_INFO(log, "logical time buffer has been used more than half, t_next updated to: {}", t_next); // TODO: replace with metrics couting how many times t_next is updated
        }
        else
        {
            /// No update for physical time
            LOG_INFO(log, "No update for physical time: [t_now = {}], [t_next = {}], [t_last = {}], [cur_ts.physical = {}], [cur_ts.logical = {}]", host_port, t_now, t_next, t_last, cur_ts.physical, cur_ts.logical);
            return;
        }

        if (t_last <= t_next + 1)  /// current timestamp already out of TSO window, update the window
        {
            t_last = t_next + tso_window;
            /// save to KV
            proxy_ptr->setTimestamp(t_last);
            tso_service->setIsKvDown(false);
        }
        tso_service->setPhysicalTime(t_next);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::TSO_OPERATION_ERROR)
            tso_service->setIsKvDown(true);
        LOG_ERROR(log, "Exception!{}", e.message());
    }
    catch (...)
    {
        LOG_ERROR(log, "Unhandled Exception!\n{}", boost::current_exception_diagnostic_information());
        // if any other unhandled exception happens, we should terminate the current process using KillingErrorHandler
        // and let TSO service recover with another replica.
        Poco::ErrorHandler::handle();
    }
}

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, UInt16 port, Poco::Logger * log)
{
    Poco::Net::SocketAddress socket_address;
    try
    {
        socket_address = Poco::Net::SocketAddress(host, port);
    }
    catch (const Poco::Net::DNSException & e)
    {
        const auto code = e.code();
        if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                    || code == EAI_ADDRFAMILY
#endif
           )
        {
            LOG_ERROR(log, "Cannot resolve listen_host ({}), error {}: {}. "
                "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                "specify IPv4 address to listen in <listen_host> element of configuration "
                "file. Example: <listen_host>0.0.0.0</listen_host>",
                host, e.code(), e.message());
        }

        throw;
    }
    return socket_address;
}

Poco::Net::SocketAddress TSOServer::socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure) const
{
    auto address = makeSocketAddress(host, port, &logger());
#if !defined(POCO_CLICKHOUSE_PATCH) || POCO_VERSION < 0x01090100
    if (secure)
        /// Bug in old (<1.9.1) poco, listen() after bind() with reusePort param will fail because have no implementation in SecureServerSocketImpl
        /// https://github.com/pocoproject/poco/pull/2257
        socket.bind(address, /* reuseAddress = */ true);
    else
#endif
#if POCO_VERSION < 0x01080000
    socket.bind(address, /* reuseAddress = */ true);
#else
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config().getBool("listen_reuse_port", false));
#endif

    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(&logger(), "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(/* backlog = */ config().getUInt("listen_backlog", 64));

    return address;
}

void TSOServer::onLeader()
{

    /// wait for exit old leader before calling syncTSO()
    auto sleep_time = config().getInt64("tso_service.election_check_ms", 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));

    syncTSO();
    timer.start(callback);

    LOG_INFO(log, "Current node {} become leader", host_port);
    tso_service->setIsLeader(true);
};


void TSOServer::exitLeaderElection()
{
    LOG_INFO(log, "Exit leader election");

    if (tso_service->getIsLeader())
    {
        num_yielded_leadership++;
    }
    tso_service->setIsLeader(false);
    leader_election.reset();
    current_zookeeper.reset();
    timer.stop();
}

void TSOServer::enterLeaderElection()
{
    try
    {
        LOG_DEBUG(log, "Enter leader election");

        auto election_path = config().getString("tso_service.election_path", TSO_ELECTION_DEFAULT_PATH);

        current_zookeeper = global_context->getZooKeeper();
        current_zookeeper->createAncestors(election_path + "/");

        leader_election = std::make_shared<zkutil::LeaderElection>(
            global_context->getSchedulePool(),
            election_path,
            *current_zookeeper,
            [&]() { return onLeader(); },
            host_port,
            false
        );
    }
    catch (...)
    {
        /// Zookeeper maybe not ready now
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool TSOServer::getIsLeaderFromTSOService() const
{
    return tso_service->getIsLeader();
}

void TSOServer::createServer(
    const std::string & listen_host,
    const char * port_name,
    bool listen_try,
    CreateServerFunc && func)
{
    /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
    if (!config().has(port_name))
        return;

    auto port = config().getInt(port_name);
    try
    {
        keeper_servers.emplace_back(func(port));
        keeper_servers.back()->start();
        LOG_INFO(&logger(), "Listening for {}", keeper_servers.back()->getDescription());
    }
    catch (const Poco::Exception &)
    {
        std::string message = "Listen [" + listen_host + "]:" + std::to_string(port) + " failed: " + getCurrentExceptionMessage(false);

        if (listen_try)
        {
            LOG_WARNING(&logger(), "{}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                " Example for disabled IPv4: <listen_host>::</listen_host>",
                message);
        }
        else
        {
            throw Exception{message, ErrorCodes::NETWORK_ERROR};
        }
    }
}

int TSOServer::main(const std::vector<std::string> &)
{
#if !defined(NDEBUG) || !defined(__OPTIMIZE__)
    LOG_WARNING(log, "Keeper was built in debug mode. It will work slowly.");
#endif

#if defined(SANITIZER)
    LOG_WARNING(log, "Keeper was built with sanitizer. It will work slowly.");
#endif

    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->initCnchConfig(config());
    global_context->initServiceDiscoveryClient();
    global_context->setApplicationType(Context::ApplicationType::TSO);

    auto service_discovery = global_context->getServiceDiscoveryClient();

    tso_port = config().getUInt("tso_service.port", 7070);
    const std::string & tso_host = getHostIPFromEnv();
    host_port = createHostPortString(tso_host, tso_port);

    if (host_port.empty())
        LOG_WARNING(log, "host_port is empty. Please set PORT0 and TSO_IP env variables for consul/dns mode. For local mode, check cnch-server.xml");
    else
        LOG_TRACE(log, "host_port: {}", host_port);

    proxy_ptr = std::make_shared<TSOProxy>(MetastoreConfig{config(), TSO_SERVICE_CONFIGURE});
    tso_service = std::make_shared<TSOImpl>();

    bool listen_try = config().getBool("listen_try", false);
    auto listen_hosts = getMultipleValuesFromConfig(config(), "", "listen_host");

    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
        listen_hosts.emplace_back(tso_host);
        listen_try = true;
    }

    Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));
    if (config().has("keeper_server"))
    {
        /// Initialize keeper RAFT.
        global_context->initializeKeeperDispatcher(false);
        FourLetterCommandFactory::registerCommands(*global_context->getKeeperDispatcher());

        for (const auto & listen_host : listen_hosts)
        {
            /// TCP Keeper
            const char * port_name = "keeper_server.tcp_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port) -> ProtocolServerAdapterPtr
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC));
                socket.setSendTimeout(config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC));
                return std::make_shared<ProtocolServerAdapter>(
                    port_name,
                    "Keeper (tcp): " + address.toString(),
                    std::make_unique<Poco::Net::TCPServer>(
                        new KeeperTCPHandlerFactory(*this, false), server_pool, socket));
            });

            const char * secure_port_name = "keeper_server.tcp_port_secure";
            createServer(listen_host, secure_port_name, listen_try, [&](UInt16 port) -> ProtocolServerAdapterPtr
            {
#if USE_SSL
                Poco::Net::SecureServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC));
                socket.setSendTimeout(config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC));
                return std::make_shared<ProtocolServerAdapter>(
                    secure_port_name,
                    "Keeper with secure protocol (tcp_secure): " + address.toString(),
                    std::make_unique<Poco::Net::TCPServer>(
                        new KeeperTCPHandlerFactory(*this, true), server_pool, socket));
#else
                UNUSED(port);
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif
            });
        }
    }

    bool enable_leader_election = global_context->hasZooKeeper();
    if (enable_leader_election)
    {
        startLeaderElection(global_context->getSchedulePool());
        tso_service->setExitLeaderElectionFunction(std::bind(&TSOServer::exitLeaderElection, this));
    }
    else
    {
        /// Enable tso without leader election if there are only one tso-server(TODO: check).
        /// Sync time with KV && Launch thread to update tso
        syncTSO();
        timer.start(callback);
        tso_service->setIsLeader(true);
    }

    static KillingErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// launch brpc service on multiple interface
    std::vector<std::unique_ptr<brpc::Server>> rpc_servers;
    for (const auto & listen : listen_hosts)
    {
        rpc_servers.push_back(std::make_unique<brpc::Server>());
        auto & rpc_server = rpc_servers.back();

        if (rpc_server->AddService(tso_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG_ERROR(log, "Failed to add rpc service.");
            throw Exception("Failed to add rpc service.", ErrorCodes::TSO_INTERNAL_ERROR);
        }
        LOG_INFO(log, "Added rpc service");

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;

        std::string brpc_listen_interface = createHostPortString(listen, tso_port);
        if (rpc_server->Start(brpc_listen_interface.c_str(), &options) != 0)
        {
            if (listen_try)
                LOG_WARNING(log, "Failed to start TSO server on address: {}", brpc_listen_interface);
            else
                throw Exception("Failed to start TSO server on address: " + brpc_listen_interface, ErrorCodes::TSO_INTERNAL_ERROR);
        }
        else
            LOG_INFO(log, "TSO Service is listening on address {}", brpc_listen_interface);
    }

    // zkutil::EventPtr unused_event = std::make_shared<Poco::Event>();
    // zkutil::ZooKeeperNodeCache unused_cache([] { return nullptr; });
    // /// ConfigReloader have to strict parameters which are redundant in our case
    // auto main_config_reloader = std::make_unique<ConfigReloader>(
    //     config_path,
    //     "",
    //     config().getString("path", ""),
    //     std::move(unused_cache),
    //     unused_event,
    //     [&](ConfigurationPtr config, bool /* initial_loading */)
    //     {
    //         if (config->getBool("enable_keeper", false))
    //             global_context->updateKeeperConfiguration(*config);
    //     },
    //     /* already_loaded = */ false);  /// Reload it right now (initial loading)

    std::unique_ptr<HTTPServer> http_server;

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    Poco::Timespan keep_alive_timeout(config().getUInt("tso_service.http.keep_alive_timeout", 10), 0);
    http_params->setKeepAliveTimeout(keep_alive_timeout);

    for (const auto & listen_host : listen_hosts)
    {
        auto create_server = [&](const char * port_name, auto && func)
        {
            /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
            if (!config().has(port_name))
                return;

            auto port = config().getInt(port_name);
            try
            {
                func(port);
            }
            catch (const Poco::Exception &)
            {
                std::string message = "Listen [" + listen_host + "]:" + std::to_string(port) + " failed: " + getCurrentExceptionMessage(false);

                if (listen_try)
                {
                    LOG_WARNING(&logger(), "{}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                        "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                        "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                        " Example for disabled IPv4: <listen_host>::</listen_host>",
                        message);
                }
                else
                {
                    throw Exception{message, ErrorCodes::NETWORK_ERROR};
                }
            }
        };

        create_server("tso_service.http.port", [&](UInt16 port)
        {

            Poco::Net::ServerSocket socket;

            auto address = socketBindListen(socket, listen_host, port);
            socket.setReceiveTimeout(config().getInt("tso_service.http.receive_timeout", DEFAULT_HTTP_READ_BUFFER_TIMEOUT));
            socket.setSendTimeout(config().getInt("tso_service.http.send_timeout", DEFAULT_HTTP_READ_BUFFER_TIMEOUT));

            auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>("TSO-PrometheusHandler-factory");
            auto handler = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
                *this, std::make_shared<TSOPrometheusMetricsWriter>(config(), *this, global_context, "tso_service.http.prometheus"));
            handler->attachStrictPath(config().getString("prometheus.endpoint", "/metrics"));
            handler->allowGetAndHeadRequest();
            factory->addHandler(handler);

            http_server = std::make_unique<HTTPServer>(
                global_context,
                factory,
                server_pool,
                socket,
                http_params);


            LOG_INFO(&logger(), "Listening http://{}", address.toString());
        });
    }
    if (http_server)
        http_server->start();


    SCOPE_EXIT({
        LOG_INFO(log, "Shutting down.");
        /// Stop reloading of the main config. This must be done before `global_context->shutdown()` because
        /// otherwise the reloading may pass a changed config to some destroyed parts of ContextSharedPart.
        // main_config_reloader.reset();

        if (restart_task)
            restart_task->deactivate();

        global_context->shutdown();

        LOG_DEBUG(log, "Waiting for current connections to Keeper to finish.");
        int current_connections = 0;
        for (auto & keeper_server : keeper_servers)
        {
            keeper_server->stop();
            current_connections += keeper_server->currentConnections();
        }

        if (current_connections)
            LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
        else
            LOG_INFO(log, "Closed all listening sockets.");

        if (current_connections > 0)
            current_connections = waitServersToFinish(keeper_servers, config().getInt("shutdown_wait_unfinished", 5));

        if (current_connections)
            LOG_INFO(log, "Closed connections to Keeper. But {} remain. Probably some users cannot finish their connections after context shutdown.", current_connections);
        else
            LOG_INFO(log, "Closed connections to Keeper.");

        if (http_server)
        {
            http_server->stop();
            LOG_INFO(log, "Stop HTTP metrics server.");
        }


        global_context->shutdownKeeperDispatcher();

        /// Wait server pool to avoid use-after-free of destroyed context in the handlers
        server_pool.joinAll();

        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context.reset();
        shared_context.reset();

        LOG_DEBUG(log, "Destroyed global context.");

        if (current_connections)
        {
            LOG_INFO(log, "Will shutdown forcefully.");
            forceShutdown();
        }
    });

    waitForTerminationRequest();
    return Application::EXIT_OK;
}

int TSOServer::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(TSOServer::options());
        std::stringstream header;
        header << "Eg : " << commandName() << " --config-file /etc/usr/config.xml";
        help_formatter.setHeader(header.str());
        help_formatter.format(std::cout);
        return 0;
    }
    if (config().hasOption("version"))
    {
        std::cout << "CNCH TSO server version " << TSO_VERSION << "." << std::endl;
        return 0;
    }
    return ServerApplication::run();
}
}

}

int mainEntryClickHouseTSOServer(int argc, char ** argv)
{
    DB::TSO::TSOServer server;
    try
    {
        return server.run(argc, argv);
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 0;
    }
}
