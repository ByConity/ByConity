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

namespace DB
{

class InternalResourceGroup : public IResourceGroup
{
public:

    ResourceGroupType getType() const override { return DB::ResourceGroupType::Internal; }
    bool canRunMore() const override;
    bool canQueueMore() const override;

    void initCpu();

    Int32 getCpuShares() const { return cpu_shares; }
    void setCpuShares(Int32 cpu_shares_) { cpu_shares = cpu_shares_; }
    
    Int64 getCfsQuota() const { return cfs_quota; }
    void setCfsQuota(Int64 cfs_quota_) { cfs_quota = cfs_quota_; }
    int initCfsQuota();

    int setCfsQuotaPeriod(Int64 cfs_quota_, Int64 cfs_period_);

    Int64 getCfsPeriod() const { return cfs_period; }
    void setCfsPeriod(Int64 cfs_period_ ) { cfs_period = cfs_period_; }
    int initCfsPeriod();

    String getCGroupName()
    {
        String result = name;
        IResourceGroup * group = this;
        while (group->getParent())
        {
            group = group->getParent();
            result.insert(0, group->getName() + "/");
        }
        return result;
    }

private:
    CpuControllerPtr cpu{nullptr};

    Int32 cpu_shares{0};
    Int64 cfs_quota{0};
    Int64 cfs_period{0};
};

}
