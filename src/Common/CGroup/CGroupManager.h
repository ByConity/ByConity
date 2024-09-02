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

#include <filesystem>
#include <unordered_map>
#include <utility>
#include <unistd.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/CGroup/CpuController.h>
#include <Common/CGroup/CpuSet.h>
#include <Common/Config/ConfigProcessor.h>

namespace DB
{
class CGroupManager
{
public:
    bool enable(){ return access("/proc/cgroups", F_OK) == 0 && access(cgroup_root_path.c_str(), F_OK) == 0; }

    bool enableForCpu(){ return enable(); }

    /// cpuset

    CpuSetPtr getCpuSet(const String & cpu_set_name);

    CpuSetPtr getSystemCpuSet(){ return getCpuSet(SYSTEM); }

    CpuSetPtr createCpuSet(const String & cpu_set_name, const Cpus & cpus);

    CpuSetPtr createCpuSet(const String & cpu_set_name, const String & cpus);

    void destroyCpuSet(const String & cpu_set_name);

    void destroyCpuSet(const CpuSet & cpu_set);

    void scaleCpuSet(CpuSet & cpu_set, const Cpus & cpus);

    void init();

    bool isInit() { return init_flag; }

    /// cpu
    CpuControllerPtr createCpu(const String & cpu_name, const UInt64 share);

    CpuControllerPtr getCpu(const String & cpu_name);

public:
    void setCGroupRootPath(String cgroup_root_path_) {cgroup_root_path = cgroup_root_path_;}
    inline String getCgroupRootPath() {return cgroup_root_path;}

    void setDockerPath(String docker_path_) {docker_path = docker_path_;}
    inline String getDockerPath() {return docker_path;}

    String getCGroupCpuSetPath() 
    {
        if (docker_path.empty())
        {
            return cgroup_root_path + "/cpuset";
        }
        else
        {
            return cgroup_root_path + "/cpuset/" + docker_path;
        }
    }
    inline String getClickhouseCpuSetPath() { return getCGroupCpuSetPath() + "/clickhouse";}

    String getCGroupCpuPath()
    {
        if (docker_path.empty())
        {
            return cgroup_root_path + "/cpu,cpuacct";
        }
        else
        {
            return cgroup_root_path + "/cpu,cpuacct/" + docker_path;
        }
    }
    inline String getClickhouseCpuPath() { return getCGroupCpuPath() + "/clickhouse"; }

private:
    struct PassKey
    {
        explicit PassKey() {}
    };

    std::unordered_map<String, CpuSetPtr> cpu_set_map;
    std::unordered_map<String, CpuControllerPtr> cpu_map;
    std::recursive_mutex mutex;
    std::atomic<bool> init_flag = false;

    static const String CGROUP_ROOT_PATH;
    static const String SYSTEM;

    String cgroup_root_path = CGROUP_ROOT_PATH;
    String docker_path;

    void alloc(const Cpus & cpus);

    void free(const Cpus & cpus);

    void initCpuSetRoot();

    void initClickhouseCpuSet();

    void moveClickhouseProc();

    friend class CGroupManagerFactory;
    friend class CpuSetScaleOperator;

public:
    explicit CGroupManager(PassKey pass_key);
    CpuControllerPtr getCpu(const String & cpu_name, const UInt64 share);
};

using CGroupManagerPtr = std::shared_ptr<CGroupManager>;
}

