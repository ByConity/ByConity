#include <Common/SystemUtils.h>
#include <filesystem>
#include <fstream>

namespace DB
{
constexpr auto linux_numa_cpu_file = "/sys/devices/system/node/has_cpu";
size_t max_numa_node = 0;

__attribute__((constructor)) static void init_max_numa_node()
{
    if (!std::filesystem::exists(linux_numa_cpu_file))
    {
        return;
    }

    std::ifstream fstream(linux_numa_cpu_file);
    std::stringstream buffer;
    buffer << fstream.rdbuf();

    max_numa_node = std::stoul(buffer.str().substr(buffer.str().find('-') + 1));
}
}
