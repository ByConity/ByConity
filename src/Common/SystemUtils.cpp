#include <Common/SystemUtils.h>
#include <filesystem>
#include <fstream>

namespace DB
{
/// Nodes that have one or more CPUs.
constexpr auto linux_numa_cpu_file_has_cpu = "/sys/devices/system/node/has_cpu";
/// Nodes that are online.
constexpr auto linux_numa_cpu_file_online = "/sys/devices/system/node/online";
/// Nodes that could be possibly become online at some point.
constexpr auto linux_numa_cpu_file_possible = "/sys/devices/system/node/possible";
size_t max_numa_node = 0;

size_t buffer_to_number(const std::string & buffer)
{
    try
    {
        return std::stoul(buffer.substr(buffer.rfind('-') + 1));
    }
    catch (...)
    {
        return 0;
    }
}

/// Try best to init `max_numa_node` with the max numa node number we have at current time.
__attribute__((constructor)) static void init_max_numa_node()
{
    auto try_read_max_numa_nude = [](const char * file_name) -> bool {
        if (!std::filesystem::exists(file_name))
            return false;

        std::ifstream fstream(linux_numa_cpu_file_online);
        std::stringstream buffer;
        buffer << fstream.rdbuf();

        if (buffer.str().empty())
            return false;

        max_numa_node = buffer_to_number(buffer.str());
        return true;
    };

    if (try_read_max_numa_nude(linux_numa_cpu_file_has_cpu))
        return;

    if (try_read_max_numa_nude(linux_numa_cpu_file_online))
        return;

    try_read_max_numa_nude(linux_numa_cpu_file_possible);
}
}
