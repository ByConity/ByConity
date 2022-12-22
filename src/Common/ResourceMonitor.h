#pragma once

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

public:
    struct Data
    {
        UInt64 total_ticks;
        UInt64 active_ticks;
        double cpu_usage;
    };

    CPUMonitor();
    ~CPUMonitor();

    Data get();

private:
    int fd;
    Data data{};
};

class MemoryMonitor
{
    static constexpr auto filename = "/proc/meminfo";

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
private:
    int fd;
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
