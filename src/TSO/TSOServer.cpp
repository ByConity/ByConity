#include <TSO/TSOServer.h>
#include <Poco/Util/HelpFormatter.h>
#include <brpc/server.h>
#include <common/logger_useful.h>
#include <gflags/gflags.h>
#include <chrono>
#include <ServiceDiscovery/ServiceDiscoveryFactory.h>
#include <ServiceDiscovery/registerServiceDiscovery.h>
#include <boost/exception/diagnostic_information.hpp>
#include <common/ErrorHandlers.h>

#include <Core/Defines.h>
#include <Common/Configurations.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Poco/Net/NetException.h>

using namespace std::chrono;

namespace brpc {namespace policy {DECLARE_string(consul_agent_addr);}}
namespace DB
{

namespace TSO
{

void TSOServer::defineOptions(Poco::Util::OptionSet &_options)
{
    Application::defineOptions(_options);

    _options.addOption(
        Poco::Util::Option("help", "h", "show help and exit")
            .required(false)
            .repeatable(false)
            .binding("help"));

    _options.addOption(
        Poco::Util::Option("config-file", "", "set config file path")
            .required(false)
            .repeatable(false)
            .argument("config-file", true)
            .binding("config-file"));

}

void TSOServer::initialize(Poco::Util::Application & self)
{

    BaseDaemon::initialize(self);

    log = &logger();

    DB::registerServiceDiscovery();

    const char * consul_http_host = getenv("CONSUL_HTTP_HOST");
    const char * consul_http_port = getenv("CONSUL_HTTP_PORT");
    if (consul_http_host != nullptr && consul_http_port != nullptr)
        brpc::policy::FLAGS_consul_agent_addr = "http://" + String(consul_http_host) + ":" + String(consul_http_port);

    tso_window = config().getInt("tso_service.tso_window_ms", 3000);  /// 3 seconds
    tso_max_retry_count = config().getInt("tso_service.tso_max_retry_count", 3); // TSOV: see if can keep or remove

    service_disovery = ServiceDiscoveryFactory::instance().create(config());

    // as shared by devops  "consul mode is for in-house k8s cluster, dns mode is for on-cloud k8s cluster. local mode is for local test and CI"
    if(service_disovery->getName() != "local")
    {
        const char * rpc_port = getenv("PORT0");
        const char * tso_host = getenv("TSO_IP");
        if(rpc_port != nullptr && tso_host != nullptr)
        {
            port = atoi(rpc_port);
            host_port = createHostPortString(tso_host, port);
        }
    }
    else
    {
        // use non obvious default values so that when there is an error retrieving values out of config, we can spot it quickly
        port = config().getUInt("tso_service.port", 7070);
        host_port = createHostPortString(config().getString("service_discovery.tso.node.host", "127.0.0.2"), port);
    }

    if (host_port.empty())
        LOG_WARNING(log, "Hostport is empty. Please set PORT0 and TSO_IP env variables for consul/dns mode. For local mode, check cnch-server.xml");
    LOG_TRACE(log, "hostport : {}", host_port);

    TSOConfig tso_config;
    tso_config.service_name = config().getString("tso_service.bytekv.service_name", "toutiao.bytekv.proxy.service.lq");
    tso_config.cluster_name = config().getString("tso_service.bytekv.cluster_name", "user_test");
    tso_config.name_space = config().getString("tso_service.bytekv.name_space", "olap_cnch_test");
    tso_config.table_name = config().getString("tso_service.bytekv.table_name", "cnch_tso");
    tso_config.key_name = config().getString("tso_service.bytekv.key_name", "tso");

    proxy_ptr = std::make_shared<TSOProxy>(tso_config);
    tso_service = std::make_shared<TSOImpl>();

    /// Sync time with KV
    syncTSO();
    /// Launch thread to update tso
    timer.start(callback);
}

void TSOServer::syncTSO()
{
    try
    {
        /// get current unix timestamp
        milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        UInt64 Tnow = ms.count();
        Tnext = Tnow;

        /// get timestamp from KV
        UInt64 Tlast_prev = 0;
        proxy_ptr->getTimestamp(Tlast_prev);

        if (Tlast_prev == 0)
        {
            if (Tnext == 0)
            {
                Tnext = 1;  /// avoid MVCC version is zero
            }
            Tlast = Tnext + tso_window;
        }
        else
        {
            if (Tnow < Tlast_prev + 1)  /// unix timestamp is in TSO window, then update Tnext to Tlast
            {
                Tnext = Tlast_prev + 1;
            }
            Tlast = Tnext + tso_window;
        }

        /// save to KV
        proxy_ptr->setTimestamp(Tlast);
        /// sync to tso service
        tso_service->setPhysicalTime(Tnext);
    }
    catch(Exception & e)
    {
        LOG_ERROR(log, e.message());
    }
}

void TSOServer::updateTSO(Poco::Timer &)
{
    try
    {
        /// get current unix timestamp
        milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        UInt64 Tnow = ms.count();
        TSOClock cur_ts = tso_service->getClock();

        if (Tnow > Tnext + 1)  /// machine time is larger than physical time, keep physical time close to machine time
        {
            Tnext = Tnow;
        }
        else if (cur_ts.logical > MAX_LOGICAL / 2)  /// logical time buffer has been used more than half, increase physical time and clear logical time
        {
            Tnext = cur_ts.physical + 1;
            LOG_INFO(log, "logical time buffer has been used more than half, Tnext updated to: {}", Tnext); // TODO: replace with metrics couting how many times Tnext is updated
        }
        else
        {
            /// No update for physical time
            return;
        }

        if (Tlast <= Tnext + 1)  /// current timestamp already out of TSO window, update the window
        {
            Tlast = Tnext + tso_window;
            /// save to KV
            proxy_ptr->setTimestamp(Tlast);
        }
        tso_service->setPhysicalTime(Tnext);
    }
    catch(Exception & e)
    {
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

int TSOServer::main(const std::vector<std::string> &)
{
    /// launch brpc service
    brpc::Server server;
    static KillingErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    if (server.AddService(tso_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE)!=0)
    {
        LOG_ERROR(log, "Failed to add rpc service.");
        exit(-1);
    }
    LOG_INFO(log, "Added rpc service");

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;


    std::string brpc_listen_interface = createHostPortString("::", port);
    if (server.Start(brpc_listen_interface.c_str(), &options) != 0)
    {
        LOG_ERROR(log, "Failed to start TSO server on address: {}", brpc_listen_interface);
        exit(-1);
    }

    LOG_INFO(log, "TSO Service start on address {}", brpc_listen_interface);

    waitForTerminationRequest();
    return Application::EXIT_OK;
}

int TSOServer::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter helpFormatter(TSOServer::options());
        std::stringstream header;
        header << "Eg : " << commandName() << " --config-file /etc/usr/config.xml";
        helpFormatter.setHeader(header.str());
        helpFormatter.format(std::cout);
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
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}

