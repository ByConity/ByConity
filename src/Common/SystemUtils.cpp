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

std::mutex numa_nodes_cpu_mask_mutex;
bool numa_nodes_cpu_mask_initialized = false;
std::vector<cpu_set_t> numa_nodes_cpu_mask;

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

std::vector<size_t> parse_cpu_list(const std::string & cpu_list_str)
{
    std::unique_ptr<DB::UInt16> lb_cache = nullptr;
    DB::Int32 digit_cache = -1;
    std::vector<size_t> cpu_list;
    for (auto it = cpu_list_str.cbegin();; it++)
    {
        if (it == cpu_list_str.cend() || *it == ',')
        {
            if (digit_cache < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid format of cpu_list: {}", cpu_list_str);
            if (!lb_cache)
                cpu_list.emplace_back(digit_cache);
            else
            {
                auto start = *lb_cache.release();

                for (int i = start; i <= digit_cache; i++)
                {
                    cpu_list.emplace_back(i);
                }
            }
            if (it == cpu_list_str.cend())
                break;
            digit_cache = -1;
        }
        else if (*it >= '0' && *it <= '9')
        {
            digit_cache = digit_cache > 0 ? digit_cache * 10 + (*it - 48) : (*it - 48);
        }
        else if (std::isspace(*it))
        {
            
        }
        else if (*it == '-')
        {
            if (digit_cache < 0 || lb_cache)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid format of cpu_list: {}", cpu_list_str);
            lb_cache = std::make_unique<DB::UInt16>(digit_cache);
            digit_cache = -1;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid format of cpu_list: {}", cpu_list_str);
    }
    return cpu_list;
}

void init_numa_nodes_cpu_mask()
{
    numa_nodes_cpu_mask.resize(max_numa_node+1);

    for (size_t numa_node = 0; numa_node < numa_nodes_cpu_mask.size(); ++numa_node)
    {
        CPU_ZERO(&numa_nodes_cpu_mask[numa_node]);

        std::string cpu_list_path = fmt::format("/sys/devices/system/node/node{}/cpulist", numa_node);
        if (!std::filesystem::exists(cpu_list_path))
            continue;
        std::ifstream fstream(cpu_list_path);
        std::stringstream buffer;
        buffer << fstream.rdbuf();
        if (buffer.str().empty())
            continue;

        try
        {
            auto cpu_list = parse_cpu_list(buffer.str());
            for (auto cpu_index : cpu_list)
                CPU_SET(cpu_index, &numa_nodes_cpu_mask[numa_node]);
        }
        catch (std::exception &)
        {
        }
    }
}

std::vector<cpu_set_t> SystemUtils::getNumaNodesCpuMask()
{
#if defined(__linux__)
    if (numa_nodes_cpu_mask_initialized)
        return numa_nodes_cpu_mask;
    std::unique_lock lock(numa_nodes_cpu_mask_mutex);
    if (numa_nodes_cpu_mask_initialized)
        return numa_nodes_cpu_mask;
    init_numa_nodes_cpu_mask();
    numa_nodes_cpu_mask_initialized = true;
    return numa_nodes_cpu_mask;
#else
    return {};
#endif
}

}
