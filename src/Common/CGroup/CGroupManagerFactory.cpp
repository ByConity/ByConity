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

#include <Common/CGroup/CGroupManagerFactory.h>
#include <iostream>
#include <fcntl.h>
#include <common/find_symbols.h>
#include <Common/SystemUtils.h>

namespace DB
{

CGroupManagerPtr CGroupManagerFactory::cgroup_manager_instance = std::make_shared<CGroupManager>(CGroupManager::PassKey());

CGroupManager & DB::CGroupManagerFactory::instance()
{
    return *cgroup_manager_instance;
}

void CGroupManagerFactory::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has("enable_cgroup") || !config.getBool("enable_cgroup"))
        return;

    LOG_INFO(getLogger("CGroupManager"), "Init CGroupManager");

    if (config.has("cgroup_root_path") && !config.getString("cgroup_root_path").empty())
    {
        CGroupManagerFactory::instance().setCGroupRootPath(config.getString("cgroup_root_path"));
    }
    String docker_path = getDockerPath();
    if (!docker_path.empty() && (docker_path != "/"))
    {
        CGroupManagerFactory::instance().setDockerPath(docker_path);
    }

    CGroupManagerFactory::instance().init();
}

String CGroupManagerFactory::getDockerPath()
{
    String cgroup_content;
    SystemUtils::ReadFileToString("/proc/" + std::to_string(getpid()) + "/cgroup", cgroup_content);

    String result;

    std::vector<String> split_lines;
    splitInto<'\n'>(split_lines, cgroup_content);
    for (auto & line : split_lines)
    {
        if (line.find("cpuacct") != std::string::npos)
        {
            std::vector<String> split_line;
            splitInto<':'>(split_line, line);
            result = (split_line.size() == 3) ? split_line[2] : "";
            break;
        }
    }

    return result;
}

}
