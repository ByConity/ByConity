#pragma once

#include <Common/Config/ConfigProcessor.h>
#include <daemon/BaseDaemon.h>
#include <TSO/TSOImpl.h>
#include <TSO/TSOProxy.h>
#include <Poco/Timer.h>
#include <ServiceDiscovery/IServiceDiscovery.h>


#define TSO_VERSION "1.0.0"

namespace DB
{

namespace TSO
{


class TSOServer : public BaseDaemon
{

public:
    using ServerApplication::run;

    using TSOProxyPtr = std::shared_ptr<TSOProxy>;
    using TSOServicePtr = std::shared_ptr<TSOImpl>;

    TSOServer ();

    ~TSOServer() override = default;

    void defineOptions(Poco::Util::OptionSet & _options) override;

    void initialize(Poco::Util::Application &) override;

    void syncTSO();

    void updateTSO(Poco::Timer &);

    String getHostPort() const { return host_port; }
protected:
    int run() override;

    int main(const std::vector<std::string> & args) override;

private:
    Poco::Logger * log;

    size_t tso_window;
    Int32 tso_max_retry_count; // TSOV: see if can keep or remove

    int port;
    String host_port;

    TSOProxyPtr proxy_ptr;
    TSOServicePtr tso_service;

    UInt64 Tnext;  /// TSO physical time
    UInt64 Tlast;  /// TSO physical time upper bound (persist in KV)

    using ServiceDiscoveryClientPtr = std::shared_ptr<IServiceDiscovery>;
    mutable ServiceDiscoveryClientPtr service_disovery;
};

}

}
