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

#include <fstream>
#include <chrono>
#include <boost/lexical_cast.hpp>
#include <Common/ResourceMonitor.h>

#include <Common/CurrentMetrics.h>
#include <Common/filesystemHelpers.h>
#include <common/getFQDNOrHostName.h>
#include <common/getMemoryAmount.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <WorkerTasks/ManipulationList.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <ResourceManagement/CommonData.h>

namespace 
{
template<class T>
std::optional<T> getNumberFromFile(const char * filename) 
{
    std::ifstream istream(filename);
    if (istream.is_open())
    {
        T val;
        std::string line;
        if (std::getline(istream, line))
        {
            try
            {
                val = boost::lexical_cast<T>(line);
            }
            catch (...)
            {
                return std::nullopt;
            }
            return val;
        }
    }
    return std::nullopt;
};
template<class T>
std::optional<T> getNumberFromFileByName(const char * filename, std::string name)
{
    name += " ";
    std::ifstream istream(filename);

    if (istream) {
        std::string line;
        while(std::getline(istream, line))
        {
            if (line.find(name) != std::string::npos)
            {
                line = line.replace(0, name.length(), "");
                T val;
                try
                {
                    val = boost::lexical_cast<T>(line);
                }
                catch (...)
                {
                    return std::nullopt;
                }
                return val;
            }
        }
    }
    return std::nullopt;
};
bool inContainer()
{
    const static std::string PID_ONE_CGROUP = "/proc/1/cgroup";
    const static std::string CONTAINER_SIGNS[] = {"kubepods", "docker", "lxc", "openshift"};
    std::ifstream file(PID_ONE_CGROUP);
    if (file) 
    {
        std::string line;
        while (std::getline(file, line)) 
        {
            for (const auto & sign : CONTAINER_SIGNS)
            {
                if (line.find(sign) != std::string::npos)
                    return true;
            }
        }
    }
    return false;
}
}

namespace CurrentMetrics
{
    extern const Metric Consumer;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

CPUMonitor::CPUMonitor()
{
    fd = ::open(filename, O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrno("Cannot open file " + std::string(filename), errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    in_container = inContainer(); 
    LOG_DEBUG(getLogger(__PRETTY_FUNCTION__), "The env is in container : {}", in_container);
}

CPUMonitor::~CPUMonitor()
{
    if (0 != ::close(fd))
        tryLogCurrentException(__PRETTY_FUNCTION__);
}

std::optional<CPUMonitor::ContainerData> CPUMonitor::getContainerData()
{
    auto cfs_quota_us_val = getNumberFromFile<Int64>(cfs_quota_us_fs);
    auto cfs_period_us_val = getNumberFromFile<UInt64>(cfs_period_us_fs);
    auto cpu_usage_val = getNumberFromFile<UInt64>(cpu_usage_fs);
    if (cfs_quota_us_val && cfs_period_us_val && cpu_usage_val)
    {
        if (*cfs_quota_us_val == -1 || *cfs_period_us_val == 0)
            return std::nullopt;
        
        auto now = std::chrono::system_clock::now();
        auto wall_time_diff = std::chrono::duration_cast<std::chrono::microseconds>(now - container_data.last_time).count();

        auto cpu_time_diff = *cpu_usage_val - container_data.last_cpu_time; //nano time

        container_data.last_time = now;
        container_data.last_cpu_time = *cpu_usage_val;

        auto all_cpu_wall_time = (*cfs_quota_us_val / *cfs_period_us_val) * wall_time_diff;
        if (all_cpu_wall_time == 0)
            return std::nullopt;
        container_data.cpu_usage = (cpu_time_diff / 10.0) / (all_cpu_wall_time);

        cpu_usage_accumulate += likely(buffer.full()) ? data.cpu_usage - buffer.front() : data.cpu_usage;
        buffer.push_back(data.cpu_usage);
        data.cpu_usage_avg_1min = cpu_usage_accumulate / buffer.size();

        return container_data;
    }
    return std::nullopt;
}

CPUMonitor::CommonData CPUMonitor::get()
{
    if (in_container)
    {
        auto tmp_container_data = getContainerData();
        if (tmp_container_data)
            return *tmp_container_data;
    }
    return getPhysicalMachineData();
}

CPUMonitor::Data CPUMonitor::getPhysicalMachineData()
{
    size_t buf_size = 1024;
    char buf[buf_size];

    ssize_t res = 0;
    int retry = 3;

    while (retry--)
    {
        res = ::pread(fd, buf, buf_size, 0);
        if (-1 == res)
        {
            if (errno == EINTR)
                continue;

            throwFromErrno("Cannot read from file " + std::string(filename), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res >= 0)
            break;
    }

    if (res < 0)
        throw Exception("Can't open file " + std::string(filename), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);

    ReadBufferFromMemory in(buf, res);

    uint64_t prev_active_ticks = data.active_ticks;
    uint64_t prev_total_ticks = data.total_ticks;

    // Remove cpu label
    skipNonNumericIfAny(in);
    data.total_ticks = 0;

    uint64_t idle_ticks = 0;
    for (auto i = 0; i < 8; i++)
    {
        /// Iterate through first 8 CPU time components (Guest is already included in user and nice)
        uint64_t tmp;
        readIntText(tmp, in);
        skipWhitespaceIfAny(in);
        data.total_ticks += tmp;
        if (i == 3)
            idle_ticks = tmp;
    }
    data.active_ticks = data.total_ticks - idle_ticks;

    // FIXME: Using sync interval as CPU Usage intervals
    uint64_t active_ticks_in_interval = data.active_ticks - prev_active_ticks;
    uint64_t total_ticks_in_interval = data.total_ticks - prev_total_ticks;
    data.cpu_usage = 100.00 * active_ticks_in_interval / total_ticks_in_interval;
    
    cpu_usage_accumulate += likely(buffer.full()) ? data.cpu_usage - buffer.front() : data.cpu_usage;
    buffer.push_back(data.cpu_usage);
    data.cpu_usage_avg_1min = cpu_usage_accumulate / buffer.size();

    return data;
}


MemoryMonitor::MemoryMonitor()
{
    fd = ::open(filename, O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrno("Cannot open file " + std::string(filename), errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    in_container = inContainer();
    LOG_DEBUG(getLogger(__PRETTY_FUNCTION__), "The env is in container : {}", in_container);
}


MemoryMonitor::~MemoryMonitor()
{
    if (0 != ::close(fd))
        tryLogCurrentException(__PRETTY_FUNCTION__);
}

std::optional<MemoryMonitor::Data> MemoryMonitor::getContainerData()
{
    Data data{};
    auto mem_usage_in_bytes_val = getNumberFromFile<UInt64>(mem_usage_fs);
    auto mem_limit_val = getNumberFromFile<UInt64>(mem_limit_fs);
    auto mem_total_inactive_file_val = getNumberFromFileByName<UInt64>(mem_stat_fs, "total_inactive_file");

    if (mem_usage_in_bytes_val && mem_limit_val && mem_total_inactive_file_val)
    {
        // referring to the memory usage method in k8s, use memory.usage_in_bytes - total_inactive_file.
        auto mem_working_usage_val = *mem_usage_in_bytes_val - *mem_total_inactive_file_val;
        
        data.memory_total = *mem_limit_val;
        data.memory_available = data.memory_total - mem_working_usage_val;
        data.memory_usage = 100.00 * static_cast<double>(data.memory_total - data.memory_available) / data.memory_total;

        memory_usage_accumulate += likely(buffer.full()) ? data.memory_usage - buffer.front() : data.memory_usage;
        buffer.push_back(data.memory_usage);
        data.memory_usage_avg_1min = memory_usage_accumulate / buffer.size();

        return data;
    }
    return std::nullopt;
}

MemoryMonitor::Data MemoryMonitor::get()
{
    if (in_container)
    {
        auto data = getContainerData();
        if (data)
            return *data;
    }
    return getPhysicalMachineData();
}

MemoryMonitor::Data MemoryMonitor::getPhysicalMachineData()
{
    Data data{};

    size_t buf_size = 1024;
    char buf[buf_size];

    ssize_t res = 0;
    int retry = 3;

    while (retry--)
    {
        res = ::pread(fd, buf, buf_size, 0);

        if (-1 == res)
        {
            if (errno == EINTR)
                continue;

            throwFromErrno("Cannot read from file " + std::string(filename), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res >= 0)
            break;
    }

    if (res < 0)
        throw Exception("Can't open file " + std::string(filename), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);

    ReadBufferFromMemory in(buf, res);

    String tmp;
    readWord(tmp, in);
    if (tmp != "MemTotal:")
        throw Exception("Unexpected element " + tmp + " while parsing meminfo", ErrorCodes::LOGICAL_ERROR);
    skipWhitespaceIfAny(in);
    readIntText(data.memory_total, in);
    // Skip MemFree line
    readString(tmp, in);
    skipWhitespaceIfAny(in);
    readString(tmp, in);
    skipWhitespaceIfAny(in);
    readWord(tmp, in);
    if (tmp != "MemAvailable:")
        throw Exception("Unexpected element " + tmp + " while parsing meminfo", ErrorCodes::LOGICAL_ERROR);
    skipWhitespaceIfAny(in);
    readIntText(data.memory_available, in);

    if (!data.memory_total)
        throw Exception("Total memory is 0", ErrorCodes::LOGICAL_ERROR);

    data.memory_usage = 100.00 * static_cast<double>(data.memory_total - data.memory_available) / data.memory_total;
    
    memory_usage_accumulate += likely(buffer.full()) ? data.memory_usage - buffer.front() : data.memory_usage;
    buffer.push_back(data.memory_usage);
    data.memory_usage_avg_1min = memory_usage_accumulate / buffer.size();

    return data;
}

UInt64 ResourceMonitor::getCPULimit()
{
    if (getenv("POD_CPU_CORE_LIMIT"))
        return std::stoi(getenv("POD_CPU_CORE_LIMIT"));
    return getNumberOfPhysicalCPUCores();
}

UInt64 ResourceMonitor::getMemoryLimit()
{
    if (getenv("POD_MEMORY_BYTE_LIMIT"))
        return std::stoi(getenv("POD_MEMORY_BYTE_LIMIT"));
    return getMemoryAmount();
}

UInt64 ResourceMonitor::getDiskSpace()
{
    auto path = getContext()->getPath();
    auto stat = getStatVFS(path);
    auto available_bytes = stat.f_bavail * stat.f_frsize;
    return available_bytes;
}

UInt64 ResourceMonitor::getQueryCount()
{
    return getContext()->getProcessList().size(); /// TODO: remove system_query.
}

UInt64 ResourceMonitor::getManipulationTaskCount()
{
    return getContext()->getManipulationList().size();
}

UInt64 ResourceMonitor::getConsumerCount()
{
    return CurrentMetrics::values[CurrentMetrics::Consumer];
}

WorkerNodeResourceData ResourceMonitor::createResourceData(bool init)
{
    WorkerNodeResourceData data;

    data.host_ports = getContext()->getHostWithPorts();

    auto cpu_data = cpu_monitor.get();
    auto mem_data = mem_monitor.get();

    data.cpu_usage = cpu_data.cpu_usage;
    data.memory_usage = mem_data.memory_usage;
    data.memory_available = mem_data.memory_available;
    data.disk_space = getDiskSpace();
    data.query_num = getQueryCount();
    data.last_status_create_time = time(nullptr);
    data.manipulation_num = getManipulationTaskCount();
    data.consumer_num = getConsumerCount();
    data.register_time = start_time;

    if (init)
    {
        data.cpu_limit = getCPULimit();
        data.memory_limit = getMemoryLimit();
    }

    return data;
}

}
