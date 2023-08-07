#pragma once

#include <mutex>
#include <Catalog/CatalogConfig.h>
#include <Catalog/MetastoreProxy.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>
#include "CatalogConfig.h"
#include "ExternalCatalog/IExternalCatalog.h"
#include "IExternalCatalogMgr.h"
#include "Transaction/TxnTimestamp.h"
namespace DB::ExternalCatalog
{

// For debug usage.
class CnchExternalCatalogMgr : public IExternalCatalogMgr
{
public:
    CnchExternalCatalogMgr(Context & context, const Poco::Util::AbstractConfiguration & conf);
    ~CnchExternalCatalogMgr() override = default;

    bool createCatalog(const std::string & catalog_name, PlainConfigs * catalog_meta, const TxnTimestamp & ts) override;
    ExternalCatalogPtr getCatalog(const std::string & catalog_name) override;
    ExternalCatalogPtr tryGetCatalog(const std::string & catalog_name) override;
    bool isCatalogExist(const std::string & catalog_name) override;
    bool dropExternalCatalog(const std::string & catalog_name) override;
    bool alterExternalCatalog(const std::string & catalog_name, PlainConfigs * changes) override;

private:
    std::mutex mu;
    [[maybe_unused]] Context & context;
    Catalog::CatalogConfig metastore_conf;
    std::shared_ptr<Catalog::MetastoreProxy>  meta_proxy;      // connection to fdb/bytekv. 
    std::string name_space;
    std::map<std::string, PlainConfigsPtr> catalog_confs; // use for check whether the catalog has been changed.
    std::map<std::string, ExternalCatalogPtr> catalogs; // map from catalog name to externcal catalog ptr.
    Poco::Logger * log = &Poco::Logger::get("CnchExternalCatalogMgr");
};
}
