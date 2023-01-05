#pragma once

#include <ResourceGroup/IResourceGroup.h>

namespace Poco { class Logger; }

namespace DB
{

class QueryStatus;
class Context;
class CnchTopologyMaster;
using CnchTopologyMasterPtr = std::shared_ptr<CnchTopologyMaster>;

/** Resource group which has root groups for each Virtual Warehouse
  * Child classes (further filters) are currently not yet supported
  */
class VWResourceGroup : public IResourceGroup, protected WithContext 
{
public:
    VWResourceGroup(ContextPtr context_);

    ResourceGroupType getType() const override { return DB::ResourceGroupType::VirtualWarehouse; }
    bool canRunMore() const override;
    bool canQueueMore() const override;

    void setSyncExpiry(Int64 expiry) { sync_expiry = expiry; }
    Int64 getSyncExpiry() const { return sync_expiry; }

private:
    Int32 getNumServers() const;

    UInt64 sync_expiry;
    mutable CnchTopologyMasterPtr topology_master;
    mutable std::atomic<bool> logged = false;
    mutable std::atomic<bool> running_limit_debug_logged = false;
    mutable std::atomic<bool> queued_limit_debug_logged = false;
    Poco::Logger * log;
};

}
