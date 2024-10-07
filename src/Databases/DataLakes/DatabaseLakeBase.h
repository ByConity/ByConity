#pragma once

#include <Common/Logger.h>
#include <filesystem>
#include <Core/MultiEnum.h>
#include <Core/NamesAndTypes.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

class DatabaseLakeBase : public IDatabase
{
public:
    ~DatabaseLakeBase() override = default;

    DatabaseLakeBase(const String & database_name_, const String & metadata_path_, const ASTStorage * database_engine_define_);

    bool canContainMergeTreeTables() const override final { return false; }

    bool canContainDistributedTables() const override final { return false; }

    bool shouldBeEmptyOnDetach() const override final { return false; }

    void shutdown() override final { }

    void drop(ContextPtr /*context*/) override final { std::filesystem::remove_all(getMetadataPath()); }

    String getMetadataPath() const override final { return metadata_path; }

    ASTPtr getCreateDatabaseQuery() const override final;

    StoragePtr tryGetTable(const String & name, ContextPtr context) const override final;

    void createTable(ContextPtr /*context*/, const String & /*table_name*/, const StoragePtr & /*storage*/, const ASTPtr & /*create_query*/)
        override final
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "createTable is not supported");
    }
    void dropTable(ContextPtr /*context*/, const String & /*table_name*/, bool /*no_delay*/) override final
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "dropTable is not supported");
    }

    void loadStoredObjects(ContextMutablePtr /*context*/, bool /*has_force_restore_data_flag*/, bool /*force_attach*/) override final { }

    void attachTable(const String & /*table_name*/, const StoragePtr & /*storage*/, const String & /*relative_table_path*/) override final
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "attachTable is not supported");
    }
    StoragePtr detachTable(const String & /*table_name*/) override final
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "detachTable is not supported");
    }
    void detachTablePermanently(ContextPtr /*context*/, const String & /*table_name*/) override final
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "detachTablePermanently is not supported");
    }

protected:
    virtual ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr context, bool throw_on_error) const override = 0;
    static void addCreateQuerySettings(ASTCreateQuery & create_query, const CnchHiveSettingsPtr & storage_settings);

    String metadata_path;
    ASTPtr database_engine_define;
    CnchHiveSettingsPtr storage_settings;

private:
    LoggerPtr log{getLogger("DatabaseLakeBase")};
};

class LakeDatabaseTablesIterator : public IDatabaseTablesIterator
{
public:
    LakeDatabaseTablesIterator(const std::string & name, const std::vector<StoragePtr> & all_tables_, const std::vector<String> & names)
        : IDatabaseTablesIterator(name), all_tables(all_tables_), all_names(names)
    {
    }
    LakeDatabaseTablesIterator(const std::string & name, std::vector<StoragePtr> && all_tables_, std::vector<String> & names)
        : IDatabaseTablesIterator(name), all_tables(std::move(all_tables_)), all_names(std::move(names))
    {
    }
    void next() override { ++index; }
    bool isValid() const override { return index < all_tables.size(); }
    const String & name() const override { return all_names[index]; }
    const StoragePtr & table() const override { return all_tables[index]; }


private:
    std::vector<StoragePtr> all_tables;
    std::vector<String> all_names;
    size_t index = 0;
};
}
