#pragma once

#include <Common/Logger.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage_fwd.h>
#include "ExternalCatalog/IExternalCatalog.h"

#include <shared_mutex>

#include <unordered_set>


namespace Poco
{
class Logger;
}


namespace DB
{

/* Database to store StorageDictionary tables
 * automatically creates tables for all dictionaries
 */
class DatabaseExternalHive final : public IDatabase, WithContext
{
public:
    DatabaseExternalHive(const String & catalog_name, const String & name_, ContextPtr context_);

    String getEngineName() const override { return "HiveExternal"; }

    bool isTableExist(const String & table_name, ContextPtr context_) const override;

    StoragePtr tryGetTable(const String & table_name, ContextPtr context_) const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) override;

    bool empty() const override;

    ASTPtr getCreateDatabaseQuery() const override;

    bool shouldBeEmptyOnDetach() const override { return false; }

    void shutdown() override;

protected:
    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr local_context, bool throw_on_error) const override;

private:
    String hive_catalog_name;
    ExternalCatalog::ExternalCatalogPtr hive_catalog;
    LoggerPtr log;
    mutable std::unordered_map<String, std::tuple<StoragePtr,time_t>> cache;
    mutable std::shared_mutex cache_mutex;
};

}
