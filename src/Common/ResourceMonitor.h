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

#pragma once
#include <optional>
#include <string>
#include <time.h>

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <boost/circular_buffer.hpp>
#include "common/types.h"

namespace DB
{

class Context;
namespace ResourceManagement
{
    struct WorkerNodeResourceData;
}
using WorkerNodeResourceData = ResourceManagement::WorkerNodeResourceData;

class CPUMonitor
{
    static constexpr auto filename = "/proc/stat";
    static constexpr auto cfs_quota_us_fs = "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us";
    static constexpr auto cfs_period_us_fs = "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us";
    static constexpr auto cpu_usage_fs = "/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage";

public:
    struct CommonData
    {
        double cpu_usage;
        double cpu_usage_avg_1min;
        double cpu_usage_avg_10sec;
    };
    struct Data : public CommonData
    {
        UInt64 total_ticks;
        UInt64 active_ticks;
    };
    struct ContainerData : public CommonData
    {
        std::chrono::system_clock::time_point last_time;
        UInt64 last_cpu_time;
    };

    CPUMonitor();
    ~CPUMonitor();

    Data getPhysicalMachineData();
    std::optional<ContainerData> getContainerData();
    CPUMonitor::CommonData get();

private:
    int fd;
    boost::circular_buffer<double> buffer{60};
    boost::circular_buffer<double> buffer_10sec{10};
    double cpu_usage_accumulate_10sec{0};
    double cpu_usage_accumulate{0};
    Data data{};
    ContainerData container_data{};
    bool in_container {false};
};

class MemoryMonitor
{
    static constexpr auto filename = "/proc/meminfo";
    static constexpr auto mem_usage_fs = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
    static constexpr auto mem_limit_fs = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
    static constexpr auto mem_stat_fs = "/sys/fs/cgroup/memory/memory.stat";

public:
    struct Data
    {
        UInt64 memory_total;
        UInt64 memory_available;
        double memory_usage;
        double memory_usage_avg_1min;
    };

    MemoryMonitor();
    ~MemoryMonitor();

    Data get();
    std::optional<Data> getContainerData();
    Data getPhysicalMachineData();

private:
    int fd;
    bool in_container {false};
    boost::circular_buffer<double> buffer{60};
    double memory_usage_accumulate{0};
};

class ResourceMonitor : protected WithContext
{
public:
    explicit ResourceMonitor(const ContextPtr global_context_) : WithContext(global_context_), mem_monitor(), cpu_monitor()
    {
        start_time = time(nullptr);
    }

    WorkerNodeResourceData createResourceData(bool init = false);

    inline UInt32 getStartTime() const
    {
        return start_time;
    }

private:
    UInt64 getCPULimit();
    UInt64 getMemoryLimit();

    UInt64 getDiskSpace();
    UInt64 getQueryCount();
    UInt64 getManipulationTaskCount();
    UInt64 getConsumerCount();

    MemoryMonitor mem_monitor;
    CPUMonitor cpu_monitor;
    UInt32 start_time;
};

}
