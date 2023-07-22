#pragma once
#include <memory>
#include <vector>

namespace Poco::Util {
    class AbstractConfiguration;
}

namespace Poco {
    class Logger;
}

class IClient;
class LoadBalancer;

/* start UDF servers and return clients to each server */
std::vector<std::unique_ptr<IClient>>
startServers(const Poco::Util::AbstractConfiguration *cfg,
             Poco::Logger *log,
             const char *udf_path);

void stopServers();

/* register loadbalancer to get notified once crash server restarted */
void registerLoadBalancer(LoadBalancer *lb);

/* call before destroy the loadbalancer */
void unregisterLoadBalancer();
