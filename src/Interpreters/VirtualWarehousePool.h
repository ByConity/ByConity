#pragma once

#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Interpreters/Context_fwd.h>
#include <Common/ConcurrentMapForCreating.h>

#include <boost/noncopyable.hpp>
#include <common/logger_useful.h>

namespace DB
{
class Context;

/**
 *  VirtualWarehousePool is a pool which will cache the used VW.
 *  So the cached handles might be outdated, the outdated handles will be replaced or removed.
 */
class VirtualWarehousePool : protected ConcurrentMapForCreating<std::string, VirtualWarehouseHandleImpl>, protected WithContext, private boost::noncopyable
{
public:
    VirtualWarehousePool(ContextPtr global_context_);

    VirtualWarehouseHandle get(const String & vw_name);

    using ConcurrentMapForCreating::erase;
    using ConcurrentMapForCreating::size;
    using ConcurrentMapForCreating::getAll;

private:
    VirtualWarehouseHandle creatorImpl(const String & vw_name);

    bool tryGetVWFromRM(const String & vw_name, VirtualWarehouseData & vw_data);
    bool tryGetVWFromCatalog(const String & vw_name, VirtualWarehouseData & vw_data);
    bool tryGetVWFromServiceDiscovery(const String & vw_name, VirtualWarehouseData & vw_data);

    void removeOrReplaceOutdatedVW();

    Poco::Logger * log {};

    std::atomic<UInt64> last_update_time_ns{0};
};

}
