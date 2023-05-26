#include "InMemoryExternalCatalogMgr.h"
#include <utility>
#include <Core/Types.h>
#include <Transaction/TxnTimestamp.h>
#include <Poco/Util/MapConfiguration.h>
#include "Common/Exception.h"
#include "ExternalCatalog/IExternalCatalogMgr.h"
#include "MockExternalCatalog.h"
namespace DB::ErrorCodes
{
extern const int UNKNOWN_CATALOG;
}
namespace DB::ExternalCatalog
{
bool InMemoryExternalCatalogMgr::createCatalog(
    const std::string & catalog_name, PlainConfigs * properties, [[maybe_unused]] const TxnTimestamp & ts)
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
ExternalCatalogPtr InMemoryExternalCatalogMgr::getCatalog(const std::string & catalog_name)
{
    auto ret = tryGetCatalog(catalog_name);
    if (!ret)
        throw Exception("The catalog " + catalog_name + " dose not exsit.", ErrorCodes::UNKNOWN_CATALOG);
    return ret;
}


ExternalCatalogPtr InMemoryExternalCatalogMgr::tryGetCatalog(const std::string & catalog_name)
{
    std::lock_guard lock(mu);
    auto it = catalogs.find(catalog_name);
    if (it != catalogs.end())
    {
        return std::make_shared<MockExternalCatalog>(catalog_name);
    }
    return nullptr;
}
bool InMemoryExternalCatalogMgr::isCatalogExist(const std::string & catalog_name)
{
    std::lock_guard lock(mu);
    auto it = catalogs.find(catalog_name);
    return it != catalogs.end();
}

std::vector<std::pair<String, CatalogProperties>> InMemoryExternalCatalogMgr::listCatalog()
{
    std::vector<std::pair<String, CatalogProperties>> ret;
    std::lock_guard lock(mu);
    ret.reserve(catalogs.size());
    for (const auto & e : catalogs)
    {
        ret.emplace_back(std::make_pair(e.first, CatalogProperties{}));
    }
    return ret;
}

std::optional<String> InMemoryExternalCatalogMgr::getCatalogCreateQuery(const std::string & catalog_name)
{
    return "CREATE EXTERNAL CATALOG " + catalog_name;
}

bool InMemoryExternalCatalogMgr::dropExternalCatalog(const std::string & catalog_name)
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
bool InMemoryExternalCatalogMgr::alterExternalCatalog(const std::string & catalog_name, PlainConfigs * changes)
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
}