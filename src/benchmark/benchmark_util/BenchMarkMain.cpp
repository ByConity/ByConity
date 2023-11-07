#include <cstdlib>
#include <string_view>
#include <stdlib.h>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#include <benchmark/benchmark.h>
#pragma clang diagnostic pop
#include <benchmark/benchmark_util/BenchMarkConfig.h>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Logger.h>

int main(int argc, char ** argv)
{
    const char * env_num_threads = std::getenv(DB::ENV_NUM_THREADS);
    if (env_num_threads != nullptr)
        DB::BenchmarkConfig::num_threads = atoi(env_num_threads);

    const char * env_logfile_path = std::getenv(DB::ENV_LOGFILE_PATH);
    if (env_logfile_path != nullptr)
        DB::BenchmarkConfig::logfile_path = std::string_view(env_logfile_path);

    Poco::AutoPtr<Poco::FileChannel> channel(new Poco::FileChannel());
    channel->setProperty("path", DB::BenchmarkConfig::logfile_path.data());
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("information");

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}
