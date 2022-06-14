#pragma once

#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{

/* Class to provide operations for CNCH tables where metadata is stored in external storage
   And it doesn't manage its own tables
 */


class DatabaseCnch : public IDatabase, protected WithContext
{
public:
    DatabaseCnch(const String & name, UUID uuid, ContextPtr context);

    String getEngineName() const override { return "Cnch"; }
    UUID getUUID() const override { return db_uuid; }
    void createTable(
        ContextPtr context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void dropTable(
        ContextPtr context,
        const String & table_name,
        bool no_delay) override;

    ASTPtr getCreateDatabaseQuery() const override;
    void drop(ContextPtr context) override;
    bool isTableExist(const String & name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;
    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name = {}) override;
    bool empty() const override;
    void shutdown() override {}

    TxnTimestamp commit_time;
private:
    const UUID db_uuid;
    Poco::Logger * log;
};

using CnchDBPtr = std::shared_ptr<DatabaseCnch>;
using CnchDatabases = std::map<String, CnchDBPtr>;

}
