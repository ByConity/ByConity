#include "InMemoryExternalCatalogMgr.h"
#include <Transaction/TxnTimestamp.h>
#include <Poco/Util/MapConfiguration.h>
#include "Common/Exception.h"
#include "MockExternalCatalog.h"

namespace DB::ErrorCodes
{
extern const int UNKNOWN_CATALOG;
}

bool DB::ExternalCatalog::InMemoryExternalCatalogMgr::createCatalog(
    const std::string & catalog_name, DB::ExternalCatalog::PlainConfigs * properties, [[maybe_unused]] const TxnTimestamp & ts)
{
    std::lock_guard lock(mu);
    auto it = catalogs.find(catalog_name);
    if (it == catalogs.end())
    {
        catalogs.emplace(catalog_name, properties);

        return true;
    }
    return false;
}
DB::ExternalCatalog::ExternalCatalogPtr DB::ExternalCatalog::InMemoryExternalCatalogMgr::getCatalog(const std::string & catalog_name)
{
    auto ret = tryGetCatalog(catalog_name);
    if (!ret)
        throw Exception("The catalog " + catalog_name + " dose not exsit.", ErrorCodes::UNKNOWN_CATALOG);
    return ret;
}

DB::ExternalCatalog::ExternalCatalogPtr DB::ExternalCatalog::InMemoryExternalCatalogMgr::tryGetCatalog(const std::string & catalog_name)
{
    std::lock_guard lock(mu);
    auto it = catalogs.find(catalog_name);
    if (it != catalogs.end())
    {
        return std::make_shared<MockExternalCatalog>(catalog_name);
    }
    return nullptr;
}
bool DB::ExternalCatalog::InMemoryExternalCatalogMgr::isCatalogExist(const std::string &catalog_name)
{
    std::lock_guard lock(mu);
    auto it = catalogs.find(catalog_name);
    return it != catalogs.end();
}
 
bool DB::ExternalCatalog::InMemoryExternalCatalogMgr::dropExternalCatalog(const std::string & catalog_name)
{
    std::lock_guard lock(mu);
    auto it = catalogs.find(catalog_name);
    if (it != catalogs.end())
    {
        catalogs.erase(it);
        return true;
    }
    return false;
}
bool DB::ExternalCatalog::InMemoryExternalCatalogMgr::alterExternalCatalog(
    const std::string & catalog_name, DB::ExternalCatalog::PlainConfigs * changes)
{
    std::lock_guard lock(mu);
    auto it = catalogs.find(catalog_name);
    if (it != catalogs.end())
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
        it->second->copyTo(*map_config);
        changes->copyTo(*map_config);
        return true;
    }
    return false;
}
