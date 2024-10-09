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
#include <chrono>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <CloudServices/CnchServerClient.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <DaemonManager/DMDefines.h>
#include <DaemonManager/DaemonFactory.h>
#include <DaemonManager/DaemonHelper.h>
#include <DaemonManager/DaemonJobGlobalGC.h>
#include <DaemonManager/DaemonManager.h>
#include <DaemonManager/Metrics/MetricsWriter.h>
#include <DaemonManager/DaemonManagerServiceImpl.h>
#include <DaemonManager/FixCatalogMetaDataTask.h>
#include <DaemonManager/registerDaemons.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Functions/registerFunctions.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/PrometheusRequestHandler.h>
#include <ServiceDiscovery/registerServiceDiscovery.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <Poco/Environment.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/HelpFormatter.h>
#include <Common/Brpc/BrpcApplication.h>
#include <Common/Config/MetastoreConfig.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/getMultipleKeysFromConfig.h>

using namespace std::chrono_literals;

#define DAEMON_MANAGER_VERSION "1.0.0"

namespace brpc
{
namespace policy
{
    DECLARE_string(consul_agent_addr);
}
}

namespace DB::ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int NETWORK_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
}

namespace DB::DaemonManager
{

static std::string getCanonicalPath(std::string && path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("path configuration parameter is empty", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (path.back() != '/')
        path += '/';
    return std::move(path);
}

void DaemonManager::uninitialize()
{
    logger().information("shutting down");
    BaseDaemon::uninitialize();
}

int DaemonManager::run()
{
    // Init Brpc config. It's a must before you construct a RpcClientBase.
    BrpcApplication::getInstance().initialize(config());
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(DaemonManager::options());
        auto header_str = fmt::format("{} [OPTION] [-- [ARG]...]\n"
                                      "positional arguments can be used to rewrite config.xml properties, for example, --http_port=8010",
                                      commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }

    if (config().hasOption("version"))
    {
        std::cout << "CNCH daemon manager version " << DAEMON_MANAGER_VERSION << "." << std::endl;
        return 0;
    }

    return Application::run();
}

void DaemonManager::initialize(Poco::Util::Application & self)
{
    BaseDaemon::initialize(self);
    logger().information("starting up");

    LOG_INFO(&logger(), "OS name: {}, version: {}, architecture: {}",
        Poco::Environment::osName(),
        Poco::Environment::osVersion(),
        Poco::Environment::osArchitecture());
}

std::string DaemonManager::getDefaultCorePath() const
{
    return getCanonicalPath(config().getString("path", DBMS_DEFAULT_PATH)) + "cores";
}

void DaemonManager::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(
        Poco::Util::Option("help", "h", "show help and exit")
            .required(false)
            .repeatable(false)
            .binding("help"));
    options.addOption(
        Poco::Util::Option("version", "V", "show version and exit")
            .required(false)
            .repeatable(false)
            .binding("version"));
    BaseDaemon::defineOptions(options);
}

std::vector<DaemonJobPtr> createLocalDaemonJobs(
    const Poco::Util::AbstractConfiguration & app_config,
    ContextMutablePtr global_context,
    LoggerPtr log)
{
    std::map<std::string, unsigned int> default_config = {
        { "GLOBAL_GC", 5000},
        { "AUTO_STATISTICS", 10000},
        { "TXN_GC", 5 * 60 * 1000},
        { "BACKUP", 10000}
    };

    std::map<std::string, unsigned int> config = updateConfig(std::move(default_config), app_config);
    LOG_INFO(log, "Local Daemon Job config:");
    printConfig(config, log);

    std::vector<DaemonJobPtr> res;

    std::for_each(config.begin(), config.end(),
        [& res, & global_context] (const std::pair<std::string, unsigned int> & config_element)
        {
            DaemonJobPtr daemon_job = DaemonFactory::instance().createLocalDaemonJob(config_element.first, global_context);
            daemon_job->setInterval(config_element.second);
            res.push_back(std::move(daemon_job));
        }
    );

    return res;
}

std::unordered_map<CnchBGThreadType, DaemonJobServerBGThreadPtr> createDaemonJobsForBGThread(
    const Poco::Util::AbstractConfiguration & app_config,
    ContextMutablePtr global_context,
    LoggerPtr log)
{
    std::unordered_map<CnchBGThreadType, DaemonJobServerBGThreadPtr> res;
    std::map<std::string, unsigned int> default_config = {
        { "PART_GC", 10000},
        { "PART_MERGE", 10000},
        { "CONSUMER", 10000},
        { "DEDUP_WORKER", 10000},
        { "PART_CLUSTERING", 10000},
        { "OBJECT_SCHEMA_ASSEMBLE", 10000},
        { "MATERIALIZED_MYSQL", 10000},
        { "CNCH_REFRESH_MATERIALIZED_VIEW", 10000},
        { "PART_MOVER", 10000},
        { "MANIFEST_CHECKPOINT", 10000}
    };

    std::map<std::string, unsigned int> config = updateConfig(std::move(default_config), app_config);
    LOG_INFO(log, "Daemon Job for server config:");
    printConfig(config, log);

    std::for_each(config.begin(), config.end(),
        [& res, & global_context] (const std::pair<std::string, unsigned int> & config_element)
        {
            DaemonJobServerBGThreadPtr daemon = DaemonFactory::instance().createDaemonJobForBGThreadInServer(config_element.first, global_context);
            daemon->setInterval(config_element.second);
            if (!res.try_emplace(daemon->getType(), daemon).second)
                throw Exception("Find duplicate daemon jobs in the config", ErrorCodes::INVALID_CONFIG_PARAMETER);

        }
    );

    return res;
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


Poco::Net::SocketAddress DaemonManager::socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure) const
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

void DaemonManager::initMetricsHandler(
    const std::vector<String> & hosts, const UInt16 & port, Poco::ThreadPool & server_pool, const bool listen_try)
{
    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    Poco::Timespan keep_alive_timeout(config().getUInt("daemon_manager.http.keep_alive_timeout", 10), 0);
    http_params->setKeepAliveTimeout(keep_alive_timeout);

    auto create_server = [&](const String & host) {
        try
        {
            Poco::Net::ServerSocket socket;

            auto address = socketBindListen(socket, host, port);
            socket.setReceiveTimeout(config().getInt("daemon_amanger_service.http.receive_timeout", DEFAULT_HTTP_READ_BUFFER_TIMEOUT));
            socket.setSendTimeout(config().getInt("daemon_amanger_service.http.send_timeout", DEFAULT_HTTP_READ_BUFFER_TIMEOUT));

            auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>("DM-PrometheusHandler-factory");
            auto handler = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
                *this, std::make_shared<DaemonManagerPrometheusMetricsWriter>());
            handler->attachStrictPath(config().getString("prometheus.endpoint", "/metrics"));
            handler->allowGetAndHeadRequest();
            factory->addHandler(handler);

            http_servers.emplace_back(std::make_unique<HTTPServer>(global_context, factory, server_pool, socket, http_params));

            http_servers.back()->start();

            LOG_INFO(&logger(), "Listening http://{}", address.toString());
        }
        catch (const Poco::Exception &)
        {
            std::string message = "Listen [" + host + "]:" + std::to_string(port) + " failed: " + getCurrentExceptionMessage(false);

            if (listen_try)
            {
                LOG_WARNING(
                    &logger(),
                    "{}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
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

    for (const auto& host : hosts)
    {
        create_server(host);
    }
}

int DaemonManager::main(const std::vector<std::string> &)
{
    DB::registerFunctions();
    DB::registerAggregateFunctions();
    DB::registerTableFunctions();
    DB::registerStorages();
    DB::registerDictionaries();
    DB::registerDisks();
    DB::registerServiceDiscovery();
    registerDaemonJobs();

    const char * consul_http_host = getConsulIPFromEnv();
    const char * consul_http_port = getenv("CONSUL_HTTP_PORT");
    if (consul_http_host != nullptr && consul_http_port != nullptr)
        brpc::policy::FLAGS_consul_agent_addr = "http://" + createHostPortString(consul_http_host, consul_http_port);

    auto log = getLogger("DM");
    LOG_INFO(log, "Daemon Manager start up...");

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases, ...
      */
    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setServerType("daemon_manager");
    global_context->initRootConfig(config());
    global_context->setSetting("background_schedule_pool_size", config().getUInt64("background_schedule_pool_size", 12));
    GlobalThreadPool::initialize(config().getUInt("max_thread_pool_size", 100));

    global_context->initCnchConfig(config());

    const Poco::Util::AbstractConfiguration & cnch_config = global_context->getCnchConfigRef();
    MetastoreConfig catalog_conf(cnch_config, CATALOG_SERVICE_CONFIGURE);
    global_context->initCatalog(catalog_conf, config().getString("catalog.name_space", "default"), config().getBool("enable_cnch_write_remote_catalog", true));
    global_context->initServiceDiscoveryClient();
    global_context->initCnchServerClientPool(config().getString("service_discovery.server.psm", "data.cnch.server"));
    global_context->initTSOClientPool(config().getString("service_discovery.tso.psm", "data.cnch.tso"));
    global_context->initTSOElectionReader();

    global_context->setCnchTopologyMaster();
    global_context->setSetting("cnch_data_retention_time_in_sec", config().getUInt64("cnch_data_retention_time_in_sec", 3*24*60*60));

    std::string path = getCanonicalPath(config().getString("path", DBMS_DEFAULT_PATH));
    global_context->setPath(path);

    HDFSConnectionParams hdfs_params = HDFSConnectionParams::parseHdfsFromConfig(config());

    /// Init HDFS3 client config path
    std::string hdfs_config = config().getString("hdfs3_config", "");
    if (!hdfs_config.empty())
    {
        setenv("LIBHDFS3_CONF", hdfs_config.c_str(), 1);
    }

    global_context->setHdfsConnectionParams(hdfs_params);

    /// Temporary solution to solve the problem with Disk initialization
    fs::create_directories(path + "disks/");

    LOG_INFO(log, "Global context initialized.");

    SCOPE_EXIT({
        global_context->shutdown();

        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context.reset();
        shared_context.reset();
        LOG_INFO(log, "Destroyed global context.");
    });

    std::unordered_map<CnchBGThreadType, DaemonJobServerBGThreadPtr> daemon_jobs_for_bg_thread_in_server =
        createDaemonJobsForBGThread(config(), global_context, log);
    std::vector<DaemonJobPtr> local_daemon_jobs = createLocalDaemonJobs(config(), global_context, log);

    {
        ThreadPool thread_pool{local_daemon_jobs.size()};
        std::for_each(local_daemon_jobs.begin(), local_daemon_jobs.end(),
            [&thread_pool] (const DaemonJobPtr & daemon_job) {
                bool scheduled = thread_pool.trySchedule([& daemon_job] ()
                    {
                        daemon_job->init();
                    }
                );

                if (!scheduled)
                    throw Exception("Failed to schedule a job", ErrorCodes::LOGICAL_ERROR);
            }
        );
        thread_pool.wait();
    }

    auto storage_cache_size = config().getUInt("daemon_manager.storage_cache_size", 1000000);
    StorageTraitCache cache(storage_cache_size); /* Cache size = storage_cache_size, invalidate an entry every 180s if unused */

    const size_t liveness_check_interval = config().getUInt("daemon_manager.liveness_check_interval", LIVENESS_CHECK_INTERVAL);

    {
        ThreadPool thread_pool{daemon_jobs_for_bg_thread_in_server.size()};

        std::for_each(
            daemon_jobs_for_bg_thread_in_server.begin(),
            daemon_jobs_for_bg_thread_in_server.end(),
            [liveness_check_interval, & cache, &thread_pool] (auto & p)
            {
                auto & daemon = p.second;
                bool scheduled = thread_pool.trySchedule([liveness_check_interval, & cache, & daemon] ()
                    {
                        /// set cache first so it can be used in init
                        daemon->setStorageTraitCache(&cache);
                        daemon->init();
                        daemon->setLivenessCheckInterval(liveness_check_interval);
                    }
                );

                if (!scheduled)
                    throw Exception("Failed to schedule a job", ErrorCodes::LOGICAL_ERROR);
            }
        );

        thread_pool.wait();
    }

    brpc::Server server;

    // launch brpc service
    int port = config().getInt("daemon_manager.port", 8090);
    std::unique_ptr<DaemonManagerServiceImpl> daemon_manager_service =
        std::make_unique<DaemonManagerServiceImpl>(daemon_jobs_for_bg_thread_in_server);

    if (server.AddService(daemon_manager_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        throw Exception("Fail to add daemon manager service.", ErrorCodes::SYSTEM_ERROR);

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    std::string host_port = createHostPortString("::", port);
    if (server.Start(host_port.c_str(), &options) != 0)
        throw Exception("Fail to start Daemon Manager RPC server.", ErrorCodes::SYSTEM_ERROR);

    /// Default port for HTTP services is at 9000.
    auto listen_hosts = getMultipleValuesFromConfig(config(), "", "listen_host");
    bool listen_try = config().getBool("listen_try", true);
    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
        listen_hosts.emplace_back("::");
        /// We want to have back compactibility for those DM who don't have such configs.
        listen_try = true;
    }
    Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));
    int metrics_port = config().getInt("daemon_manager.http.port", 9000);
    initMetricsHandler(listen_hosts, metrics_port, server_pool, listen_try);

    LOG_INFO(log, "Daemon manager service starts on address {}", host_port);

    std::for_each(
        daemon_jobs_for_bg_thread_in_server.begin(),
        daemon_jobs_for_bg_thread_in_server.end(),
        [] (auto & p)
        {
            p.second->start();
        }
    );

    std::for_each(local_daemon_jobs.begin(), local_daemon_jobs.end(),
        [] (const DaemonJobPtr & daemon_job) {
            daemon_job->start();
        }
    );

    auto fix_metadata_task = global_context->getSchedulePool().createTask("Fix catalog metadata", [log, this] () { fixCatalogMetaData(global_context, log); });
    fix_metadata_task->activateAndSchedule();
    waitForTerminationRequest();

    LOG_INFO(log, "Shutting down!");
    LOG_INFO(log, "BRPC servers stop accepting new connections and requests from existing connections");
    if (0 == server.Stop(5000))
        LOG_INFO(log, "BRPC server stop succesfully");
    else
        LOG_INFO(log, "BRPC server doesn't stop succesfully with in 5 second");

    LOG_INFO(log, "Wait until brpc requests in progress are done");
    if (0 == server.Join())
        LOG_INFO(log, "brpc joins succesfully");
    else
        LOG_INFO(log, "brpc doesn't join succesfully");


    LOG_INFO(log, "Wait for daemons for bg thread to finish.");
    std::for_each(
        daemon_jobs_for_bg_thread_in_server.begin(),
        daemon_jobs_for_bg_thread_in_server.end(),
        [] (auto & p)
        {
            p.second->stop();
        }
    );

    LOG_INFO(log, "Wait for daemons for local job to finish.");
    std::for_each(local_daemon_jobs.begin(), local_daemon_jobs.end(),
        [] (const DaemonJobPtr & daemon_job) {
            daemon_job->stop();
        }
    );

    SCOPE_EXIT({
        std::for_each(http_servers.begin(), http_servers.end(), [log](const std::unique_ptr<HTTPServer> & http_server) {
            if (http_server)
            {
                http_server->stop();
                LOG_INFO(log, "Stop HTTP metrics server.");
            }
        });
        /// Wait server pool to avoid use-after-free of destroyed context in the handlers
        server_pool.joinAll();
    });

    LOG_INFO(log, "daemons for local job finish succesfully.");

    return Application::EXIT_OK;
}

}/// end namespace DaemonManager

int mainEntryClickHouseDaemonManager(int argc, char ** argv)
{
    DB::DaemonManager::DaemonManager app;
    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
