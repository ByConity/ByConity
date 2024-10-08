#pragma once
#include <Common/Logger.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Storages/Hive/Metastore/IMetaClient.h>
#include <fmt/core.h>
#include <Poco/Logger.h>
#include "common/types.h"
#include <common/logger_useful.h>
#include "Core/Types.h"
#include "IExternalCatalog.h"

namespace DB::ExternalCatalog
{
class MockExternalCatalog : public IExternalCatalog
{
public:
    MockExternalCatalog(const std::string & _catalog_name) : catalog_name(_catalog_name) { }
    ~MockExternalCatalog() override = default;
    std::string name() override { return catalog_name; }
    std::vector<std::string> listDbNames() override { return {"mock_db1", "mock_db2"}; }
    std::vector<std::string> listTableNames(const std::string & db_name) override { return {db_name + "_table"}; }
    std::vector<std::string> listPartitionNames([[maybe_unused]] const std::string & db_name, const std::string & table_name) override
    {
        return {table_name + "_p1", table_name + "_p2"};
    }

    StoragePtr getTable(const std::string & db_name, const std::string & table_name, ContextPtr local_context) override;
    bool isTableExist(const std::string & db_name, const std::string & table_name, ContextPtr local_context) override;
    std::vector<ThriftPartition> getPartionsByFilter(
        [[maybe_unused]] const std::string & db_name,
        [[maybe_unused]] const std::string & table_name,
        [[maybe_unused]] const std::string & filter) override
    {
        return {};
    }

    UUID getTableUUID(const std::string & db_name, const std::string & table_name) override;

private:
    std::string catalog_name;
    LoggerPtr log = getLogger("MockExternalCatalog");
};

}
