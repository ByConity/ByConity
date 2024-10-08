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

#include <ResourceGroup/IResourceGroupManager.h>
#include <ResourceGroup/InternalResourceGroupManager.h>
#include <ResourceGroup/InternalResourceGroup.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <ResourceGroup/ResourceGroupSelectStrategy.h>
#include <Common/SystemUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_GROUP_ILLEGAL_CONFIG;
    extern const int RESOURCE_GROUP_MISMATCH;
}

void InternalResourceGroupManager::initialize(const Poco::Util::AbstractConfiguration &config)
{
    LOG_DEBUG(getLogger("ResourceGroupManager"), "Load resource group manager");
    if (!root_groups.empty())
    {
        LOG_WARNING(getLogger("ResourceGroupManager"), "need to restart to reload config.");
        return;
    }

    Poco::Util::AbstractConfiguration::Keys config_keys;
    String prefix = "resource_groups";
    config.keys(prefix, config_keys);
    String prefixWithKey;
    /// load resource groups
    for (const String & key : config_keys)
    {
        prefixWithKey = prefix + "." + key;
        if (key.find("resource_group") == 0)
        {
            if (!config.has(prefixWithKey + ".name"))
                throw Exception("Resource group has no name", ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);

            String name = config.getString(prefixWithKey + ".name");
            LOG_DEBUG(getLogger("ResourceGroupManager"), "Found resource group {}", name);
            if (groups.find(name) != groups.end())
                throw Exception("Resource group name duplicated: " + name, ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);

            auto pr = groups.emplace(std::piecewise_construct,
                                     std::forward_as_tuple(name),
                                     std::forward_as_tuple(std::make_shared<InternalResourceGroup>()));
            auto group = pr.first->second;
            group->setName(name);
            group->setSoftMaxMemoryUsage(config.getInt64(prefixWithKey + ".soft_max_memory_usage", 0));
            group->setMinQueryMemoryUsage(config.getInt64(prefixWithKey + ".min_query_memory_usage", 536870912)); /// default 512MB
            group->setMaxConcurrentQueries(
                    config.getInt(prefixWithKey + ".max_concurrent_queries", 0)); /// default 0
            group->setMaxQueued(config.getInt(prefixWithKey + ".max_queued", 0)); /// default 0
            group->setMaxQueuedWaitingMs(config.getInt(prefixWithKey + ".max_queued_waiting_ms", 5000)); /// default 5s
            group->setPriority(config.getInt(prefixWithKey + ".priority", 0));

            int64_t cfs_max_quota_us = -1, cfs_min_quota_us = -1, cfs_period_us = 10000;
            {
                auto internal_group = dynamic_cast<InternalResourceGroup *>(group.get());
                internal_group->setCpuShares(config.getInt64(prefixWithKey + ".cpu_shares", 0));
                cfs_period_us = config.getInt64(prefixWithKey + ".cfs_period", 10000);
                internal_group->setCfsPeriod(cfs_period_us);
                cfs_max_quota_us = config.getInt64(prefixWithKey + ".cfs_max_quota", -1);
                internal_group->setCfsQuota(cfs_max_quota_us);
                cfs_min_quota_us = config.getInt64(prefixWithKey + ".cfs_min_quota", -1);
            }
            if (config.has(prefixWithKey + ".parent_resource_group"))
            {
                String parent_name = config.getString(prefixWithKey + ".parent_resource_group");
                group->setParentName(parent_name);
            }
            else
            {
                /// root groups
                group->setRoot();
                root_groups[name] = group.get();
                struct CGroupCpuUsageInfo cpu_usage;
                cpu_usage.cfs_max_quota_us = cfs_max_quota_us;
                cpu_usage.cfs_min_quota_us = cfs_min_quota_us;
                cpu_usage.cfs_period_us = cfs_period_us;
                cpu_usage.curr_quota_us = cfs_max_quota_us;
                root_cgroup_cpu_usage[name] = cpu_usage;
            }
        }
    }
    /// Set parents
    for (auto & p : groups)
    {
        if (!p.second->getParentName().empty())
        {
            auto parent_it = groups.find(p.second->getParentName());
            if (parent_it == groups.end())
                throw Exception("Resource group's parent group not found: " + p.second->getName() + " -> " +  p.second->getParentName(), ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            p.second->setParent(parent_it->second.get());
        }
    }
    /// Set root
    std::vector<IResourceGroup*> set_root_groups;
    for (auto & p : root_groups)
    {
        set_root_groups.emplace_back(p.second);
    }
    while (!set_root_groups.empty())
    {
        std::vector<IResourceGroup*> tmp_groups;
        tmp_groups.swap(set_root_groups);
        for (auto & item : tmp_groups)
        {
            if (item->getRoot() == nullptr)
            {
                LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "init group {} root {}", 
                    item->getName(), item->getParent()->getParent()->getName());
                item->setRoot(item->getParent()->getRoot());
            }

            {
                auto internal_group = dynamic_cast<InternalResourceGroup *>(item);
                auto root_group = dynamic_cast<InternalResourceGroup *>(item->getRoot());
                internal_group->setCfsPeriod(root_group->getCfsPeriod());
                internal_group->setCfsQuota(root_group->getCfsQuota());
                internal_group->initCpu();
                internal_group->initCfsPeriod();
                internal_group->initCfsQuota();
                LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "init cgroup {} root {} period {} quota {}", 
                        internal_group->getCGroupName(), root_group->getCGroupName(), 
                        root_group->getCfsPeriod(), root_group->getCfsQuota());
            }

            for (auto child : item->getChildren())
            {
                set_root_groups.emplace_back(child.second);
            }
        }
    }
    
    /// load cases
    for (const String & key : config_keys)
    {
        prefixWithKey = prefix + "." + key;
        if (key.find("case") == 0)
        {
            ResourceSelectCase select_case;
            LOG_DEBUG(getLogger("ResourceGroupManager"), "Found resource group case {}", key);
            if (!config.has(prefixWithKey + ".resource_group"))
                throw Exception("Select case " + key + " does not config resource group", ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            select_case.name = key;
            String resourceGroup = config.getString(prefixWithKey + ".resource_group");
            auto group_it= groups.find(resourceGroup);
            if (group_it == groups.end())
                throw Exception("Select case's group not found: " + key + " -> " + resourceGroup, ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            //else if (!group_it->second->isLeaf())
            //    throw Exception("Select case's group is not leaf group: " + key + " -> " + resourceGroup, ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            select_case.group = group_it->second.get();

            if (config.has(prefixWithKey + ".user"))
                select_case.user = std::make_shared<std::regex>(config.getString(prefixWithKey + ".user"));
            if (config.has(prefixWithKey + ".query_id"))
                select_case.query_id = std::make_shared<std::regex>(config.getString(prefixWithKey + ".query_id"));
            if (config.has(prefixWithKey + ".query_type"))
            {
                String queryType = config.getString(prefixWithKey + ".query_type");
                select_case.query_type = ResourceSelectCase::translateQueryType(queryType);
                if (select_case.query_type == nullptr)
                    throw Exception("Select case's query type is illegal: " + key + " -> " + queryType, ErrorCodes::RESOURCE_GROUP_ILLEGAL_CONFIG);
            }
            select_cases[select_case.name] = std::move(select_case);
        }
    }
    LOG_DEBUG(getLogger("ResourceGroupManager"), "Found {} resource groups, {} select cases.",
              groups.size(), select_cases.size());

    select_algorithm = config.getString("resource_groups.select_algorithm", "user_query");
    if (select_algorithm == "user_query")
    {
        resource_select_algorithm = std::make_shared<UserQuerySelectStrategy>(this);
    }
    else
    {
        resource_select_algorithm = std::make_shared<UserTableSelectStrategy>(this);
    }

    bool old_val = false;
    if (!root_groups.empty()
        && started.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
    {
        resource_task = std::make_unique<ResourceTask>(this);
        timer.scheduleAtFixedRate(resource_task.get(), 1, 1);

        cpu_quota_task = std::make_unique<CGroupQuotaTask>(this);
        cpu_quota_timer.scheduleAtFixedRate(cpu_quota_task.get(), 1, 1);
    }
}

IResourceGroup * InternalResourceGroupManager::selectGroup(const Context & query_context, const IAST * ast)
{
    return resource_select_algorithm->selectGroup(query_context, ast);
}

bool InternalResourceGroupManager::cGroupQuotaSwitchOn(const String & root_group) const
{
    auto iter = root_cgroup_cpu_usage.find(root_group);
    if (iter == root_cgroup_cpu_usage.end())
        return false;

    return iter->second.cfs_max_quota_us != -1 && iter->second.cfs_min_quota_us != -1;
}

int InternalResourceGroupManager::calcCGroupCpuUsage(const String & root_group, float & usage)
{
    auto iter = root_groups.find(root_group);
    if (iter == root_groups.end())
        return -1;
    
    auto & cpu_usage_info = root_cgroup_cpu_usage[root_group];
    if ((time(nullptr) - cpu_usage_info.last_check_usage_time) < 1)
    {
        usage = cpu_usage_info.last_usage;
        return 0;
    }

    auto read_file_func = [&](const String & name)->String {
        String content;
        String path = CGroupManagerFactory::instance().getClickhouseCpuPath() 
            + "/" + root_group + "/" + name;
        SystemUtils::ReadFileToString(path, content);
        return content;
    };

    String cpu_quota_str, cpu_period_str, cpu_usage_str;
    try
    {
        cpu_quota_str = read_file_func("cpu.cfs_quota_us");
        cpu_period_str = read_file_func("cpu.cfs_period_us");
        cpu_usage_str = read_file_func("cpuacct.usage");
    }
    catch(...)
    {
        LOG_ERROR(&Poco::Logger::get("ResourceGroupManager"), "calc cgroup {} read cgroup file error", root_group);
        return -2;
    }
    
    int64_t cpu_quota = 0, cpu_period = 0, cpu_usage = 0;
    try 
    {
        cpu_quota = std::stoll(cpu_quota_str);
        cpu_period = std::stoll(cpu_period_str);
        cpu_usage = std::stoll(cpu_usage_str);
    }
    catch(...)
    {
        LOG_ERROR(&Poco::Logger::get("ResourceGroupManager"), "calc cgroup {} cgroup value invalid", root_group);
        return -3;
    }

    if (cpu_quota <= 0 || cpu_period <= 0 || cpu_usage < 0)
    {
        return -4;
    }

    float numerator = (static_cast<uint64_t>(cpu_usage) < cpu_usage_info.last_usage_value) 
            ? 0 : (static_cast<uint64_t>(cpu_usage) - cpu_usage_info.last_usage_value);
    float denominator = float((cpu_quota/cpu_period) * 1e9) * (time(nullptr) - cpu_usage_info.last_check_usage_time);

    usage = (numerator / denominator) * 100;

    cpu_usage_info.last_check_usage_time = time(nullptr);
    cpu_usage_info.last_usage = usage;
    cpu_usage_info.last_usage_value = cpu_usage;

    LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "calc cgroup {} cpu usage {}", root_group, usage);

    return 0;
}


//only set leaf nodes cfs_quota and cfs_period
int InternalResourceGroupManager::setCfsQuotaPeriod(const String & root_group, int64_t cfs_quota_us, int64_t cfs_period_us)
{
    auto get_leaf_children_func = [&](IResourceGroup* root_group_ptr) -> std::vector<IResourceGroup*> {
        std::vector<IResourceGroup*> result_groups;

        std::vector<IResourceGroup*> groups;
        for (auto & p : root_group_ptr->getChildren())
        {
            groups.emplace_back(p.second);
        }
        while (!groups.empty())
        {
            auto iter = groups.begin();
            auto item = *iter;
            groups.erase(iter);

            if (item->getChildren().empty())
            {
                result_groups.push_back(item);
            }

            for (auto child : item->getChildren())
            {
                groups.emplace_back(child.second);
            }
        }

        return result_groups;
    };

    auto iter = root_groups.find(root_group);
    assert(iter != root_groups.end());

    auto leaf_children = get_leaf_children_func(iter->second);
    for (auto & item : leaf_children)
    {
        assert(item->getRoot() != nullptr);

        auto internal_group = dynamic_cast<InternalResourceGroup *>(item);
        LOG_DEBUG(&Poco::Logger::get("ResourceGroupManager"), "process cgroup {} quota, cfs_quota_us {}, cfs_period_us {}", 
                internal_group->getCGroupName(), cfs_quota_us, cfs_period_us);
        internal_group->setCfsQuotaPeriod(cfs_quota_us, cfs_period_us);
    }

    return 0;
}

}
