#pragma once

#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>

namespace DB
{

/* Class to provide operations for CNCH tables where metadata is stored in external storage
 */

class DatabaseCnch : public DatabaseWithOwnTablesBase
{
public:
    DatabaseCnch(const String & name, ContextPtr context);

    String getEngineName() const override { return "Cnch"; }
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
};

}
