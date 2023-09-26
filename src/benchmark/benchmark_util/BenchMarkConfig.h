#pragma once

#include <cstdint>
#include <common/types.h>

namespace DB
{
constexpr char ENV_NUM_THREADS[] = "CLICKHOUSE_BENCHMARK_THREADS";

constexpr char ENV_LOGFILE_PATH[] = "CLICKHOUSE_BENCHMARK_LOGFILE_PATH";

class BenchmarkConfig
{
public:
    static UInt32 num_threads;

    static std::string_view logfile_path;
};
}
