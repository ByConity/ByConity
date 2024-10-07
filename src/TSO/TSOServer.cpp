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
#include <Poco/Util/HelpFormatter.h>
#include <brpc/server.h>
#include <Common/HostWithPorts.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/MetastoreConfig.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <chrono>
#include <memory>
#include <ServiceDiscovery/registerServiceDiscovery.h>
#include <boost/exception/diagnostic_information.hpp>
#include <common/ErrorHandlers.h>

#include <Core/Defines.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/PrometheusRequestHandler.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/TSOPrometheusMetricsWriter.h>
#include <TSO/TSOImpl.h>
#include <Poco/Net/NetException.h>
#include <Common/Brpc/BrpcApplication.h>
#include <Common/Configurations.h>
#include <Common/getMultipleKeysFromConfig.h>

using namespace std::chrono;

namespace brpc::policy
{
    DECLARE_string(consul_agent_addr);
}

namespace DB::ErrorCodes
{
    extern const int TSO_INTERNAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int TSO_OPERATION_ERROR;
}

namespace DB::TSO
{

TSOServer::TSOServer()
   : num_yielded_leadership(0)
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

    log = getLogger(logger());

    registerServiceDiscovery();

    const char * consul_http_host = getConsulIPFromEnv();
    const char * consul_http_port = getenv("CONSUL_HTTP_PORT");
    if (consul_http_host != nullptr && consul_http_port != nullptr)
        brpc::policy::FLAGS_consul_agent_addr = "http://" + createHostPortString(consul_http_host, consul_http_port);

    tso_window = config().getInt64("tso_service.tso_window_ms", 3000); /// default = 3s
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

void TSOServer::updateTSO()
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
            LOG_INFO(
                log,
                "No update for physical time on {}: [t_now = {}], [t_next = {}], [t_last = {}], [cur_ts.physical = {}], [cur_ts.logical = {}]",
                host_port, t_now, t_next, t_last, cur_ts.physical, cur_ts.logical);

            update_tso_task->scheduleAfter(TSO_UPDATE_INTERVAL);
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
    update_tso_task->scheduleAfter(TSO_UPDATE_INTERVAL);
}

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, UInt16 port, LoggerRawPtr log)
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

bool TSOServer::onLeader()
{
    tso_service->setPhysicalTime(0);
    syncTSO();
    if (update_tso_task)
        update_tso_task->activateAndSchedule();

    LOG_INFO(log, "Current node {} become leader", host_port);
    return true;
};

bool TSOServer::onFollower()
{
    num_yielded_leadership++;
    if (update_tso_task)
        update_tso_task->deactivate();

    tso_service->setPhysicalTime(0);

    LOG_INFO(log, "Current node {} become follower", host_port);
    return true;
}


bool TSOServer::isLeader() const
{
    return leader_election->isLeader();
}

void TSOServer::initLeaderElection()
{
    try
    {
        LOG_DEBUG(log, "Enter leader election");

        auto election_metastore = Catalog::getMetastorePtr(MetastoreConfig{global_context->getCnchConfigRef(), CATALOG_SERVICE_CONFIGURE});

        auto prefix = global_context->getRootConfig().service_discovery_kv.election_prefix.value;
        leader_election = std::make_unique<StorageElector>(
            std::make_shared<TSOKvStorage>(std::move(election_metastore)),
            global_context->getRootConfig().service_discovery_kv.tso_refresh_interval_ms,
            global_context->getRootConfig().service_discovery_kv.tso_expired_interval_ms,
            global_context->getHostWithPorts(),
            prefix + global_context->getRootConfig().service_discovery_kv.tso_host_path.value,
            [&](const HostWithPorts * /*host_ports*/) { return onLeader(); },
            [&](const HostWithPorts *) { return onFollower(); });
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

String TSOServer::tryGetTSOLeaderHostPort() const
{
    if (!leader_election)
        return {};

    if (auto leader_info = leader_election->getLeaderInfo())
         return createHostPortString(leader_info->getHost(), leader_info->getRPCPort());

    return {};
}

UInt64 TSOServer::getNumStopUpdateTsFromTSOService() const
{
    return tso_service->getNumTSOUpdateTsStoppedFunctioning();
}

int TSOServer::main(const std::vector<std::string> &)
{
    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->initCnchConfig(config());
    global_context->initRootConfig(config());
    global_context->initServiceDiscoveryClient();
    global_context->setApplicationType(Context::ApplicationType::TSO);
    global_context->setServerType("tso_server");

    auto service_discovery = global_context->getServiceDiscoveryClient();

    auto rpc_port = global_context->getRPCPort();
    const std::string & tso_host = getHostIPFromEnv();
    host_port = createHostPortString(tso_host, rpc_port);

    if (host_port.empty())
        LOG_WARNING(log, "host_port is empty. Please set PORT0 and TSO_IP env variables for consul/dns mode. For local mode, check cnch-server.xml");
    else
        LOG_TRACE(log, "host_port: {}", host_port);

    auto metastore_conf = MetastoreConfig{config(), TSO_SERVICE_CONFIGURE};
    auto tso_metastore = Catalog::getMetastorePtr(metastore_conf);
    proxy_ptr = std::make_shared<TSOProxy>(std::move(tso_metastore), metastore_conf.key_name);
    tso_service = std::make_shared<TSOImpl>(*this);

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

    static KillingErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// update_tso_task has to be initialized before leader election
    update_tso_task = global_context->getSchedulePool().createTask("UpdateTSOTask", [this]() { updateTSO(); });
    /// leader election for tso-server
    initLeaderElection();
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

        std::string brpc_listen_interface = createHostPortString(listen, rpc_port);
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

    std::vector<std::unique_ptr<HTTPServer>> http_servers;

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

            http_servers.push_back(std::make_unique<HTTPServer>(
                global_context,
                factory,
                server_pool,
                socket,
                http_params));


            LOG_INFO(&logger(), "Listening http://{}", address.toString());
        });
    }

    std::for_each(http_servers.begin(), http_servers.end(),
        [] (const std::unique_ptr<HTTPServer> & http_server)
        {
            if (http_server)
                http_server->start();
        }
    );

    SCOPE_EXIT({
        LOG_INFO(log, "Shutting down.");
        /// Stop reloading of the main config. This must be done before `global_context->shutdown()` because
        /// otherwise the reloading may pass a changed config to some destroyed parts of ContextSharedPart.
        // main_config_reloader.reset();

        // if (restart_task)
        //     restart_task->deactivate();

        std::for_each(rpc_servers.begin(), rpc_servers.end(),
            [this] (const std::unique_ptr<brpc::Server> & rpc_server)
            {
                if (rpc_server)
                {
                    rpc_server->Stop(1);
                    LOG_INFO(log, "Stop BRPC server.");
                }
            }
        );

        std::for_each(http_servers.begin(), http_servers.end(),
            [this] (const std::unique_ptr<HTTPServer> & http_server)
            {
                if (http_server)
                {
                    http_server->stop();
                    LOG_INFO(log, "Stop HTTP metrics server.");
                }
            }
        );


        /// Wait server pool to avoid use-after-free of destroyed context in the handlers
        server_pool.joinAll();
        if (leader_election)
            leader_election.reset();

        if (update_tso_task)
            update_tso_task->deactivate();

        global_context->shutdown();

        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context.reset();
        shared_context.reset();

        LOG_DEBUG(log, "Destroyed global context.");
    });

    waitForTerminationRequest();

    return Application::EXIT_OK;
}

int TSOServer::run()
{
    // Init Brpc config. It's a must before you construct a RpcClientBase.
    BrpcApplication::getInstance().initialize(config());
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
