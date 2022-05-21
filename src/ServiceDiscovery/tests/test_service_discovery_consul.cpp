#include <ServiceDiscovery/ServiceDiscoveryConsul.h>
#include <iostream>
#include <string>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/HostWithPorts.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>


constexpr auto HOST = "127.0.0.1";
constexpr auto HOSTNAME = "cnch-default-generate-default-0.cnch-generate-default-headless.cnch.svc.cluster.local.";
constexpr auto PORT0 = 9000;
constexpr auto PORT1 = 9001;
constexpr auto PORT2 = 9002;
constexpr auto PORT5 = 9003;
constexpr auto PORT6 = 9004;

using Endpoints = DB::ServiceDiscoveryConsul::Endpoints;

Endpoints generate_endpoints(int num_total, int num_from_other_cluster, int num_from_other_vw, std::string vw_name)
{
    Endpoints res;
    int num_valid = num_total - num_from_other_cluster - num_from_other_vw;
    // generating valid endpoint
    for (int i = 0; i < num_valid; i++)
    {
        res.emplace_back();
        res.back().host = HOST;
        res.back().port = PORT0;
        res.back().tags["hostname"] = HOSTNAME;
        res.back().tags["cluster"] = "default";
        if (!vw_name.empty())
            res.back().tags["vw_name"] = vw_name;
        res.back().tags["PORT1"] = std::to_string(PORT1);
        res.back().tags["PORT2"] = std::to_string(PORT2);
        res.back().tags["PORT5"] = std::to_string(PORT5);
        res.back().tags["PORT6"] = std::to_string(PORT6);
    }
    // generating endpoint from other cluster
    for (int i = 0; i < num_from_other_cluster; i++)
    {
        res.emplace_back();
        res.back().host = HOST;
        res.back().port = PORT0;
        res.back().tags["hostname"] = HOSTNAME;
        res.back().tags["cluster"] = "other";
        if (!vw_name.empty())
            res.back().tags["vw_name"] = vw_name;
        res.back().tags["PORT1"] = std::to_string(PORT1);
        res.back().tags["PORT2"] = std::to_string(PORT2);
        res.back().tags["PORT5"] = std::to_string(PORT5);
        res.back().tags["PORT6"] = std::to_string(PORT6);
    }
    // generating endpoints from other vw
    for (int i = 0; i < num_from_other_vw; i++)
    {
        res.emplace_back();
        res.back().host = HOST;
        res.back().port = PORT0;
        res.back().tags["hostname"] = HOSTNAME;
        res.back().tags["cluster"] = "default";
        res.back().tags["vw_name"] = "other";
        res.back().tags["PORT1"] = std::to_string(PORT1);
        res.back().tags["PORT2"] = std::to_string(PORT2);
        res.back().tags["PORT5"] = std::to_string(PORT5);
        res.back().tags["PORT6"] = std::to_string(PORT6);
    }
    return res;
}

UInt64 runFakedLookup(Endpoints & eps, std::shared_ptr<DB::ServiceDiscoveryConsul> & sd, bool show_detail, size_t expected_size)
{
    Stopwatch watch;
    auto endpoints = sd->fakedLookup(eps, "data.cnch.vw", DB::ComponentType::WORKER, "vw_default");
    watch.stop();

    if (endpoints.size() != expected_size)
    {
        std::cerr << "Lookup endpoint size incorrect: expected size [" << expected_size << "], actual size [" << endpoints.size() << "]" << std::endl;
        return 0;
    }

    UInt64 elapsed_time = watch.elapsedMicroseconds();
    if (show_detail)
        std::cout << "Lookup [" << endpoints.size() << "] endpoints, time elapsed: " << elapsed_time << " microseconds" << std::endl;

    return elapsed_time;
}

bool functionality_test(std::shared_ptr<DB::ServiceDiscoveryConsul> & sd, bool use_cache = false)
{
    std::cout << "Start functionality test." << std::endl;

    Endpoints eps_worker = generate_endpoints(10,0,0,"vw_default"); // mock workers
    Endpoints eps_non_worker = generate_endpoints(10,0,0,""); // mock non-worker components

    {  /// Test worker
        DB::HostWithPortsVec endpoints;
        if (use_cache)
        {
            sd->clearCache();
            endpoints = sd->fakedLookup(eps_worker, "data.cnch.vw", DB::ComponentType::WORKER, "vw_default");  /// Warm up cache
        }
        endpoints = sd->fakedLookup(eps_worker, "data.cnch.vw", DB::ComponentType::WORKER, "vw_default");

        if (endpoints[0].host != HOST || endpoints[0].tcp_port != PORT0 || endpoints[0].rpc_port != PORT1 || endpoints[0].http_port != PORT2)
        {
            std::cerr << "fakedLookup worker endpoints invalid" << std::endl;
            return false;
        }
    }
    {  /// Test server
        DB::HostWithPortsVec endpoints;
        if (use_cache)
        {
            sd->clearCache();
            endpoints = sd->fakedLookup(eps_non_worker, "data.cnch.server", DB::ComponentType::SERVER);  /// Warm up cache
        }
        endpoints = sd->fakedLookup(eps_non_worker, "data.cnch.server", DB::ComponentType::SERVER);

        if (endpoints[0].host != HOST || endpoints[0].tcp_port != PORT0 || endpoints[0].rpc_port != PORT1 || endpoints[0].http_port != PORT2)
        {
            std::cerr << "fakedLookup server endpoints invalid" << std::endl;
            return false;
        }
    }
    {  /// Test TSO / Daemon Manager
        DB::HostWithPortsVec endpoints;
        if (use_cache)
        {
            sd->clearCache();
            endpoints = sd->fakedLookup(eps_non_worker, "data.cnch.server", DB::ComponentType::TSO);  /// Warm up cache
        }
        endpoints = sd->fakedLookup(eps_non_worker, "data.cnch.server", DB::ComponentType::TSO);

        if (endpoints[0].host != HOST || endpoints[0].rpc_port != PORT0)
        {
            std::cerr << "fakedLookup server endpoints invalid" << std::endl;
            return false;
        }
    }

    std::cout << "Functionality test passed." << std::endl << std::endl;
    return true;
}

void performance_test(std::shared_ptr<DB::ServiceDiscoveryConsul> & sd, Endpoints & eps, size_t expected_size, size_t concurrency = 1, bool use_cache = false)
{
    std::cout << "Start performance test." << std::endl;

    /// Test `lookup` API
    ThreadPool thread_pool(concurrency);
    if (use_cache)
    {
        sd->clearCache();
        runFakedLookup(eps, sd, false, expected_size);  /// Warm up cache
    }

    std::atomic<UInt64> total_time_us {0};
    for (size_t i = 0; i < concurrency; i++)
    {
        auto task = [&]
        {
            total_time_us += runFakedLookup(eps, sd, false, expected_size);
        };

        thread_pool.trySchedule(task);
    }
    thread_pool.wait();

    std::cout << "Concurrent [" << concurrency << "] Lookup [" << expected_size << "] endpoints, avg time elapsed: " << total_time_us / concurrency << " microseconds" << std::endl;

    std::cout << "Performance test passed." << std::endl << std::endl;
}

int main(int argc, char** argv)
{
    size_t concurrency = 0;
    size_t num_eps = 0;
    size_t num_eps_other_cluster = 0;
    size_t num_eps_other_vw = 0;
    bool enable_cache = false;

    if (argc >= 3)
    {
        concurrency = atoi(argv[1]);
        num_eps = atoi(argv[2]);
        if (argc >= 4)
            num_eps_other_cluster = atoi(argv[3]);
        if (argc >= 5)
            num_eps_other_vw = atoi(argv[4]);
        if (argc >= 6)
        {
            if (strcmp(argv[5], "y") == 0 || strcmp(argv[5], "Y") == 0)
            {
                enable_cache = true;
            }
            else if (strcmp(argv[5], "n") == 0 || strcmp(argv[5], "N") == 0)
            {
                enable_cache = false;
            }
            else
            {
                std::cout << "Invalid value for [enable_cache], please enter Y or N" << std::endl;
                return -1;
            }
        }
    }
    else
    {
        std::cout << "Usage:  ./test_service_discovery_consul [concurrency] [num_total_endpoints] [num_endpoints_of_other_cluster] [num_endpoints_of_other_vw] [enable_cache (y/n)]" << std::endl;
        return -1;
    }

    using Poco::AutoPtr;
    using Poco::Util::XMLConfiguration;

    AutoPtr<XMLConfiguration> pconf(new XMLConfiguration());

    if (enable_cache)
    {
        std::cout << "Consul mode with cache" << std::endl << std::endl;
        pconf->setBool("service_discovery.disable_cache", false);
    }
    else
    {
        std::cout << "Consul mode without cache" << std::endl << std::endl;
        pconf->setBool("service_discovery.disable_cache", true);
    }

    auto sd = std::make_shared<DB::ServiceDiscoveryConsul>(*pconf.get());

    if (functionality_test(sd) == false)
        return -1;

    std::cout << "Total endpoints: " << num_eps << std::endl
              << "Endpoints of other cluster: " << num_eps_other_cluster << std::endl
              << "Endpoints of other vw: " << num_eps_other_vw << std::endl
              << "Concurrency: " << concurrency << std::endl
              << std::endl;

    Endpoints eps = generate_endpoints(num_eps, num_eps_other_cluster, num_eps_other_vw, "vw_default");
    auto expected_size = num_eps - num_eps_other_cluster - num_eps_other_vw;
    performance_test(sd, eps, expected_size, concurrency, enable_cache);

    return 0;
}
