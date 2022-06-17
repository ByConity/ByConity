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

private:
    Int32 cpu_shares{0};
    CpuControllerPtr cpu{nullptr};
};

}
