#pragma once
#include <hive_metastore_types.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <fmt/core.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "common/types.h"
#include <common/logger_useful.h>
#include "Core/Types.h"
#include "IExternalCatalog.h"
#include "IExternalCatalogMgr.h"
#include "Metastore/IMetaClient.h"
namespace DB::ExternalCatalog
{
class HiveExternalCatalog : public IExternalCatalog
{
public:
    HiveExternalCatalog(const std::string & _catalog_name, [[maybe_unused]] PlainConfigsPtr conf);
    ~HiveExternalCatalog() override = default;
    std::string name() override { return catalog_name; }
    std::vector<std::string> listDbNames() override;
    std::vector<std::string> listTableNames(const std::string & db_name) override;
    std::vector<std::string> listPartitionNames([[maybe_unused]] const std::string & db_name, const std::string & table_name) override;
    StoragePtr getTable(const std::string & db_name, const std::string & table_name, ContextPtr local_context) override;
    std::vector<ApacheHive::Partition> getPartionsByFilter(
        [[maybe_unused]] const std::string & db_name,
        [[maybe_unused]] const std::string & table_name,
        [[maybe_unused]] const std::string & filter) override;
    UUID getTableUUID(const std::string & db_name, const std::string & table_name) override;

private:
    std::string catalog_name;
    PlainConfigsPtr configs;
    IMetaClientPtr hms_client;

    //TODO(ExternalCatalog):: add storage related field.
    Poco::Logger * log = &Poco::Logger::get("HiveExternalCatalog");
};

}
