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
#include <string>
#include <optional>

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

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
    Data data{};
    ContainerData container_data{};
    bool in_container {false};
};

class MemoryMonitor
{
    static constexpr auto filename = "/proc/meminfo";
    static constexpr auto mem_usage_fs = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
    static constexpr auto mem_limit_fs = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

public:
    struct Data
    {
        UInt64 memory_total;
        UInt64 memory_available;
        double memory_usage;
    };

    MemoryMonitor();
    ~MemoryMonitor();

    Data get() const;
    std::optional<Data> getContainerData() const;
    Data getPhysicalMachineData() const;

private:
    int fd;
    bool in_container {false};
};

class ResourceMonitor : protected WithContext
{
public:
    explicit ResourceMonitor(const ContextPtr global_context_): WithContext(global_context_), mem_monitor(), cpu_monitor() {}

    WorkerNodeResourceData createResourceData(bool init = false);

private:
    UInt64 getCPULimit();
    UInt64 getMemoryLimit();

    UInt64 getDiskSpace();
    UInt64 getQueryCount();
    UInt64 getBackgroundTaskCount();

private:
    MemoryMonitor mem_monitor;
    CPUMonitor cpu_monitor;
};

}
