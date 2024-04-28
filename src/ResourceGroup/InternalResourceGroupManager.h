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

#include <ResourceGroup/IResourceGroup.h>
#include <Parsers/IAST.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Timer.h>

#include <atomic>
#include <regex>
#include <unordered_set>
#include <vector>

namespace DB
{
class IResouceGroupSelectStrategy;

class InternalResourceGroupManager : public IResourceGroupManager
{
public:
    InternalResourceGroupManager() {}
    ~InternalResourceGroupManager() override = default;

    IResourceGroup * selectGroup(const Context & query_context, const IAST * ast) override;
    void initialize(const Poco::Util::AbstractConfiguration & config) override;
    void shutdown() override {}

public:
    class CGroupQuotaTask: public Poco::Util::TimerTask
    {
    public:
        CGroupQuotaTask(InternalResourceGroupManager * manager_) : manager(manager_) {}

        virtual void run() override
        {
            static time_t last_check_time = 0;
            if ((time(nullptr) - last_check_time) <= 10)
            {
                return;
            }
            last_check_time = time(nullptr);

            for (auto & iter : manager->root_cgroup_cpu_usage)
            {
                if (!manager->cGroupQuotaSwitchOn(iter.first))
                {
                    continue;
                }

                float usage = 0;
                if (manager->calcCGroupCpuUsage(iter.first, usage) != 0)
                {
                    continue;
                }

                auto & cpu_usage_info = iter.second;
                int delta = std::max(static_cast<int>(iter.second.cfs_max_quota_us / 10), 1); //change 10% delta every time
                if (usage >= CGROUP_CPU_USAGE_FULL_THRESHHOLD && iter.second.curr_quota_us >= iter.second.cfs_min_quota_us)
                {
                    manager->setCfsQuotaPeriod(iter.first, 
                            std::max(iter.second.curr_quota_us - delta, iter.second.cfs_min_quota_us), iter.second.cfs_period_us);
                    cpu_usage_info.curr_quota_us = std::max(iter.second.curr_quota_us - delta, iter.second.cfs_min_quota_us);
                }

                if (usage < CGROUP_CPU_USAGE_FULL_THRESHHOLD && iter.second.curr_quota_us < iter.second.cfs_max_quota_us)
                {
                    manager->setCfsQuotaPeriod(iter.first, 
                            std::min(iter.second.curr_quota_us + delta, iter.second.cfs_max_quota_us), iter.second.cfs_period_us);
                    cpu_usage_info.curr_quota_us = std::min(iter.second.curr_quota_us + delta, iter.second.cfs_max_quota_us);
                }
            }
        }
    private:
        InternalResourceGroupManager * manager;
        static constexpr float CGROUP_CPU_USAGE_FULL_THRESHHOLD = 90;
    };

    bool cGroupQuotaSwitchOn(const String & root_group) const;
    int calcCGroupCpuUsage(const String & root_group, float & usage);
    int setCfsQuotaPeriod(const String & root_group, int64_t cfs_quota_us, int64_t cfs_period_us); 

private:
    struct CGroupCpuUsageInfo
    {
        time_t  last_check_usage_time = 0;
        float   last_usage = 0;
        uint64_t last_usage_value = static_cast<uint64_t>(-1);
        int64_t cfs_max_quota_us = -1;
        int64_t cfs_min_quota_us = -1;
        int64_t cfs_period_us = 10000;
        int64_t curr_quota_us = -1;
    };
    std::map<String, struct CGroupCpuUsageInfo> root_cgroup_cpu_usage;

    std::unique_ptr<CGroupQuotaTask> cpu_quota_task;
    Poco::Util::Timer cpu_quota_timer;

    std::unique_ptr<ResourceTask> resource_task;
    String select_algorithm;
    std::shared_ptr<IResouceGroupSelectStrategy> resource_select_algorithm;
};

}
