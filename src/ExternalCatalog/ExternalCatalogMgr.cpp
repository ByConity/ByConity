#include <memory>

#include "Common/Exception.h"
#include <common/logger_useful.h>
#include "CnchExternalCatalogMgr.h"
#include "IExternalCatalogMgr.h"
#include "InMemoryExternalCatalogMgr.h"
namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::ExternalCatalog
{
namespace Mgr
{
    ExternalCatalogMgrPtr mgr_ptr = nullptr;
    std::string configPrefix() {
        return  "external_catalog_mgr"; 
    }
    void init(Context & _context, [[maybe_unused]] const Poco::Util::AbstractConfiguration & conf)
    {
        //TODO(renming):: add more implementation
        auto key_mgr_type = configPrefix() + ".type";
        auto * log = &Poco::Logger::get("ExternalCatalogMgr");
        if (!conf.has(key_mgr_type))
        {
            throw Exception(fmt::format("No {} in config", key_mgr_type), ErrorCodes::BAD_ARGUMENTS);
        }
        if (conf.getString(key_mgr_type) == "memory")
        {
            // this is for mock.
            LOG_DEBUG(log, "Use in memory external catalog manager");
            mgr_ptr = std::make_unique<InMemoryExternalCatalogMgr>();
        }
        else
        {
            // this will created an catalog mgr backed by fdb/bytekv and etc.
            LOG_DEBUG(log, "Use kv-backed external catalog manager");
            mgr_ptr = std::make_unique<CnchExternalCatalogMgr>(_context,conf);
        }

        assert(mgr_ptr != nullptr);
    }

    IExternalCatalogMgr & instance()
    {
        assert(mgr_ptr != nullptr);
        return *mgr_ptr;
    }

}
}
