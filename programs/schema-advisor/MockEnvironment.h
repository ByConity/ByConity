#pragma once

#include "Interpreters/StorageID.h"
#include "MockGlobalContext.h"

#include <Analyzers/QualifiedColumnName.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/QueryPlan.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>

#include <filesystem>
#include <string>
#include <string_view>
#include <memory>
#include <vector>

namespace DB
{

class MockEnvironment
{
public:
    explicit MockEnvironment(const std::string & path, size_t max_threads);
    MockEnvironment(const MockEnvironment &other) = delete;
    ~MockEnvironment();

    // list the available databases or tables under the give path
    std::vector<std::string> listDatabases();
    std::vector<std::string> listTables(const std::string & database);
    bool containsDatabase(const std::string & database);
    bool containsTable(const std::string & database, const std::string & table);

    // get the create-table/database sql
    std::string getCreateDatabaseSql(const std::string & database);
    std::string getCreateTableSql(const std::string & database, const std::string & table);
    ColumnsDescription getColumnsDescription(const std::string & database, const std::string & table);

    // mock the query execution environment
    ContextMutablePtr createQueryContext();
    ASTPtr parse(std::string_view sql, ContextPtr query_context);
    QueryPlanPtr plan(std::string_view sql, ContextMutablePtr query_context); // no optimize
    void execute(const std::string & sql, ContextMutablePtr query_context); // supposed to execute ddl only

    void createMockDatabase(const std::string & database);
    void createMockTable(const std::string & database, const std::string & table);

    static bool isPrimaryKey(const QualifiedColumnName & column, ContextPtr context);
    static StoragePtr tryGetLocalTable(const std::string & database_name, const std::string & table_name, ContextPtr context);

private:
    ContextMutablePtr session_context;
    const std::filesystem::path actual_folder;
    const std::filesystem::path mock_folder;
    static constexpr const char * METADATA = "metadata";
    static constexpr const char * METASTORE = "metastore";
    static constexpr const char * DATA = "data";
};


} // DB
