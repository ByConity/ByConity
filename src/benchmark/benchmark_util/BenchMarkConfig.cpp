#include <benchmark/benchmark_util/BenchMarkConfig.h>

namespace DB
{
UInt32 BenchmarkConfig::num_threads = 1;
std::string_view BenchmarkConfig::logfile_path = "/tmp/clickhouse-benchmark.log";
}
