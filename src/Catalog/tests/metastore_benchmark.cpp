#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"

#include <benchmark/benchmark.h>
#include <Catalog/MetastoreFDBImpl.h>
#include <Catalog/MetastoreByteKVImpl.h>
#include <Poco/Util/XMLConfiguration.h>
#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include <string>
#include <Common/Stopwatch.h>

static std::shared_ptr<DB::Catalog::IMetaStore> metastore_ptr = nullptr;

static void BM_metastore_put(benchmark::State& state)
{
    for (auto _ : state)
        metastore_ptr->put("bench_test_key", "bench_test_value");
};

static void BM_metastore_get(benchmark::State& state)
{
    std::string value;
    for (auto _ : state)
        metastore_ptr->get("bench_test_key", value);
}


int main(int argc, char** argv) { 
    boost::program_options::options_description desc("Options");
    desc.add_options()
        ("help,h", "help list")
        ("type,t", boost::program_options::value<std::string>(), "metastore type")
        ("config,c", boost::program_options::value<std::string>(), "metastore config")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    std::string type = options["type"].as<std::string>();
    std::string config_file = options["config"].as<std::string>();

    if (type == "bytekv")
    {
        std::ifstream in(config_file);
        Poco::XML::InputSource input_source{in};
        Poco::AutoPtr<Poco::Util::AbstractConfiguration> xmlConfig = new Poco::Util::XMLConfiguration{&input_source};
        if (!xmlConfig->has("catalog_service"))
        {
            std::cerr << "config file doesn't include catalog_service tag" << std::endl;
            return -1;
        }

        metastore_ptr = std::make_shared<DB::Catalog::MetastoreByteKVImpl>(
            xmlConfig->getString("catalog_service.bytekv.service_name"),
            xmlConfig->getString("catalog_service.bytekv.cluster_name"),
            xmlConfig->getString("catalog_service.bytekv.name_space"),
            xmlConfig->getString("catalog_service.bytekv.table_name")
        );
    }
    else if (type == "fdb")
    {
        metastore_ptr = std::make_shared<DB::Catalog::MetastoreFDBImpl>(config_file);
    }
    else
    {
        std::cerr << "unsupported metastore type : " << type << std::endl;
        return -1;
    }

    BENCHMARK(BM_metastore_put)->Unit(benchmark::kMicrosecond);
    BENCHMARK(BM_metastore_get)->Unit(benchmark::kMicrosecond);                             
    ::benchmark::Initialize(&(argc), argv);              
    // if (::benchmark::ReportUnrecognizedArguments(argc, argv))
    //     return 1;
    ::benchmark::RunSpecifiedBenchmarks();
}
