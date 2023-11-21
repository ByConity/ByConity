#include "getNumberOfPhysicalCPUCores.h"

#if defined(OS_LINUX)
#include <cmath>
#include <fstream>
#endif

#include <thread>
#include <sched.h>
#include <unistd.h>

#if defined(OS_LINUX)
static int32_t readFrom(const char * filename, int default_value)
{
    std::ifstream infile(filename);
    if (!infile.is_open())
        return default_value;
    int idata;
    if (infile >> idata)
        return idata;
    else
        return default_value;
}

// Try to look at cgroups limit if it is available
unsigned getCGroupLimitedCPUCores(unsigned default_cpu_count)
{
    unsigned quota_count = default_cpu_count;
    // Return the number of milliseconds per period process is guaranteed to run.
    // -1 for no quota
    int cgroup_quota = readFrom("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", -1);
    int cgroup_period = readFrom("/sys/fs/cgroup/cpu/cpu.cfs_period_us", -1);
    if (cgroup_quota > -1 && cgroup_period > 0)
    {
        quota_count = static_cast<uint32_t>(ceil(static_cast<float>(cgroup_quota) / static_cast<float>(cgroup_period)));
    }

    return std::min(default_cpu_count, quota_count);
}

static unsigned getAffinityCnt(unsigned default_cpu_count)
{
    cpu_set_t cpu_mask = {0};

    // Get the CPU affinity mask for the calling thread
    if (sched_getaffinity(getpid(), sizeof(cpu_mask), &cpu_mask) == -1)
        return default_cpu_count;

    // Count the number of set bits in the mask to get the number of CPU cores
    unsigned num_cores = 0;
    for (int i = 0; i < __CPU_SETSIZE; i++)
    {
        if (!__CPU_ISSET_S(i, __CPU_SETSIZE, &cpu_mask))
            continue;

        num_cores++;
    }

    if (num_cores < default_cpu_count)
        return num_cores;

    return default_cpu_count;
}
#endif

static unsigned getNumberOfPhysicalCPUCoresImpl()
{
    unsigned cpu_count = std::thread::hardware_concurrency();

#if defined(OS_LINUX)
    cpu_count = getAffinityCnt(cpu_count);
#endif

    /// Most of x86_64 CPUs have 2-way Hyper-Threading
    /// Aarch64 and RISC-V don't have SMT so far.
    /// POWER has SMT and it can be multiple way (like 8-way), but we don't know how ClickHouse really behaves, so use all of them.
#if defined(__x86_64__)
    /// Let's limit ourself to the number of physical cores.
    /// But if the number of logical cores is small - maybe it is a small machine
    /// or very limited cloud instance and it is reasonable to use all the cores.
    if (cpu_count >= 32)
        cpu_count /= 2;
#endif

#if defined(OS_LINUX)
    cpu_count = getCGroupLimitedCPUCores(cpu_count);
#endif

    return cpu_count;
}


unsigned getNumberOfPhysicalCPUCores()
{
    /// Calculate once.
    static auto res = getNumberOfPhysicalCPUCoresImpl();
    return res;
}
