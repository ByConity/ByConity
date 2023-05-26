#pragma once

#include <mutex>
#include <Transaction/TxnTimestamp.h>
#include "IExternalCatalogMgr.h"


namespace DB::ExternalCatalog
{

// For debug usage.
class InMemoryExternalCatalogMgr : public IExternalCatalogMgr
{
public:
    InMemoryExternalCatalogMgr() = default;
    ~InMemoryExternalCatalogMgr() override = default;

    bool createCatalog(const std::string & catalog_name, PlainConfigs * properties, const TxnTimestamp & ts) override;
    ExternalCatalogPtr getCatalog(const std::string & catalog_name) override;
    ExternalCatalogPtr tryGetCatalog(const std::string & catalog_name) override;
    bool isCatalogExist(const std::string & catalog_name) override;
    bool dropExternalCatalog(const std::string & catalog_name) override;
    bool alterExternalCatalog(const std::string & catalog_name, PlainConfigs * changes) override;

private:
    std::mutex mu;
    std::map<std::string, Poco::AutoPtr<PlainConfigs>> catalogs;
};
}
