#include "MockEnvironment.h"
#include "MockGlobalContext.h"
#include "SchemaAdvisorHelpers.h"

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Core/UUID.h>
#include <Databases/DatabaseMemory.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParserBase.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <QueryPlan/Hints/registerHints.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/QueryPlanner.h>
#include <Statistics/CacheManager.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/registerStorages.h>

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <memory>

namespace DB
{
namespace
{
    std::string readString(const std::string & file_path)
    {
        std::ifstream fin(file_path);
        std::stringstream buffer;
        buffer << fin.rdbuf();
        return buffer.str();
    }
}

MockEnvironment::MockEnvironment(const std::string & path, size_t max_threads)
    : session_context(MockGlobalContext::instance().createSessionContext())
    , actual_folder(path)
    , mock_folder(std::filesystem::path{"/tmp"} / ("advisor_tool_" + toString(UUIDHelpers::generateV4())))
{
    session_context->setPath(mock_folder.string() + '/');
    session_context->setMetastorePath((mock_folder / METASTORE).string() + '/');

    SettingsChanges setting_changes;
    setting_changes.emplace_back("max_threads", max_threads);
    setting_changes.emplace_back("enable_memory_catalog", true);
    session_context->applySettingsChanges(setting_changes);
    std::filesystem::remove_all(mock_folder);
    std::filesystem::create_directories(mock_folder);
    std::filesystem::create_directories(mock_folder / METASTORE);
    std::filesystem::create_directories(mock_folder / METADATA);
    std::filesystem::create_directories(mock_folder / DATA);

    registerFunctions();
    registerFormats();
    registerStorages();
    registerAggregateFunctions();
    registerHints();
    registerDisks();
    Statistics::CacheManager::initialize(session_context);

    // make system database
    DatabasePtr system_database = DatabaseCatalog::instance().tryGetDatabase(DatabaseCatalog::SYSTEM_DATABASE, session_context);
    if (!system_database)
    {
        system_database = std::make_shared<DatabaseMemory>(DatabaseCatalog::SYSTEM_DATABASE, session_context);
        DatabaseCatalog::instance().attachDatabase(DatabaseCatalog::SYSTEM_DATABASE, system_database);
        attachSystemTablesLocal(*system_database);
    }
}

MockEnvironment::~MockEnvironment()
{
    for (const auto & [name, database] : DatabaseCatalog::instance().getDatabases(session_context))
    {
        for (auto it = database->getTablesIterator(session_context); it->isValid(); it->next())
        {
            database->dropTable(session_context, it->name(), /*no_delay=*/true);
        }
        database->drop(session_context);
    }

    std::filesystem::remove_all(mock_folder);
}

std::vector<std::string> MockEnvironment::listDatabases()
{
    std::vector<std::string> res;
    auto meta_path = actual_folder / METADATA;
    if (!std::filesystem::exists(meta_path))
        throw Exception("cannot find metadata", ErrorCodes::CANNOT_OPEN_FILE);
    for (const auto & file : std::filesystem::directory_iterator{meta_path})
    {
        const std::filesystem::path & fullname = file.path();
        if (fullname.extension() == ".sql")
            res.emplace_back(fullname.stem().string());
    }
    return res;
}

std::vector<std::string> MockEnvironment::listTables(const std::string & database)
{
    std::vector<std::string> res;
    auto meta_path = actual_folder / METADATA / database;
    if (!std::filesystem::exists(meta_path))
        throw Exception("cannot find metadata", ErrorCodes::CANNOT_OPEN_FILE);
    for (const auto & file : std::filesystem::directory_iterator{meta_path})
    {
        const std::filesystem::path & fullname = file.path();
        if (fullname.extension() == ".sql")
            res.emplace_back(fullname.stem().string());
    }
    return res;
}

bool MockEnvironment::containsDatabase(const std::string & database)
{
    return std::filesystem::exists(actual_folder / METADATA / (database + ".sql"));
}

bool MockEnvironment::containsTable(const std::string & database, const std::string & table)
{
    return std::filesystem::exists(actual_folder / METADATA / database / (table + ".sql"));
}

std::string MockEnvironment::getCreateDatabaseSql(const std::string & database)
{
    if (!containsDatabase(database))
        throw Exception("cannot find database " + database, ErrorCodes::CANNOT_OPEN_FILE);
    return readString(actual_folder / METADATA / (database + ".sql"));
}

std::string MockEnvironment::getCreateTableSql(const std::string & database, const std::string & table)
{
    if (!containsTable(database, table))
        throw Exception("cannot find table " + database + "." + table, ErrorCodes::CANNOT_OPEN_FILE);
    return readString(actual_folder / METADATA / database / (table + ".sql"));
}

ColumnsDescription MockEnvironment::getColumnsDescription(const std::string & database, const std::string & table)
{
    std::string create_table = getCreateTableSql(database, table);
    ContextMutablePtr context = createQueryContext();
    auto ast = parse(create_table, context)->as<ASTCreateQuery &>();
    return InterpreterCreateQuery::getColumnsDescription(*ast.columns_list->columns, context, ast.attach, false);
}

ContextMutablePtr MockEnvironment::createQueryContext()
{
    ContextMutablePtr query_context = Context::createCopy(session_context);
    query_context->createPlanNodeIdAllocator();
    query_context->createSymbolAllocator();
    query_context->makeQueryContext();
    return query_context;
}

ASTPtr MockEnvironment::parse(std::string_view sql, ContextPtr query_context)
{
    const char * begin = sql.data();
    const char * end = begin + sql.size();
    ParserQuery parser(end, ParserSettings::valueOf(query_context->getSettingsRef()));
    return parseQuery(
        parser, begin, end, "",
        query_context->getSettingsRef().max_query_size,
        query_context->getSettingsRef().max_parser_depth);
}

QueryPlanPtr MockEnvironment::plan(std::string_view sql, ContextMutablePtr query_context)
{
    ASTPtr ast = parse(sql, query_context);
    ast = QueryRewriter().rewrite(ast, query_context);
    AnalysisPtr analysis = QueryAnalyzer::analyze(ast, query_context);
    QueryPlanPtr query_plan = QueryPlanner().plan(ast, *analysis, query_context);
    return query_plan;
}

void MockEnvironment::execute(const std::string & sql, ContextMutablePtr query_context)
{
    executeQuery(sql, query_context, /*internal=*/true);
}

void MockEnvironment::createMockDatabase(const std::string & database)
{
    if (DatabaseCatalog::instance().isDatabaseExist(database, session_context))
        return;
    ContextMutablePtr query_context = createQueryContext();
    std::string sql = getCreateDatabaseSql(database);
    // the sql is "attach _ ..." in metadata, we revert it
    auto ast = dynamic_pointer_cast<ASTCreateQuery>(parse(sql, query_context));
    if (!ast)
        throw Exception("failed to create database " + database + ", invalid sql: " + sql, ErrorCodes::BAD_ARGUMENTS);
    ast->attach = false;
    ast->database = database;
    ast->uuid = UUIDHelpers::Nil;
    // there are some problems with destructing an Atomic database, so we force to memory
    if (ast->storage && ast->storage->engine)
        ast->storage->engine->name = "Memory";
    ast->cluster = "";
    execute(serializeAST(*ast), query_context);
}

void MockEnvironment::createMockTable(const std::string & database, const std::string & table)
{
    createMockDatabase(database);
    if (DatabaseCatalog::instance().getDatabase(database, session_context)->isTableExist(table, session_context))
        return;
    ContextMutablePtr query_context = createQueryContext();
    SettingsChanges setting_changes;
    setting_changes.emplace_back("enable_constraint_check", false);
    setting_changes.emplace_back("allow_nullable_key", true);
    query_context->applySettingsChanges(setting_changes);

    std::string sql = getCreateTableSql(database, table);
    // the sql is "attach _ ..." in metadata, we revert it
    auto ast = dynamic_pointer_cast<ASTCreateQuery>(parse(sql, query_context));
    if (!ast)
        throw Exception("failed to create table " + database + "." + table + ", invalid sql: " + sql, ErrorCodes::BAD_ARGUMENTS);
    ast->attach = false;
    ast->database = database;
    ast->table = table;
    ast->uuid = UUIDHelpers::Nil;
    ast->cluster = "";

    if (ast->storage && ast->storage->engine)
    {
        auto engine_name = ast->storage->engine->name;
        if (engine_name == "Distributed")
            ast->storage->engine->arguments->children[0] = std::make_shared<ASTLiteral>(MockGlobalContext::ADVISOR_SHARD);
        else if (engine_name.starts_with("Ha"))
        {
            // HaUniqueMergeTree, HaMergeTree require zookeeper
            engine_name = engine_name.substr(2, engine_name.length());
            ASTPtr mock_engine = makeASTFunction(engine_name);
            ast->storage->set(ast->storage->engine, mock_engine);
        }

        if (engine_name == "MergeTree")
        {
            ASTSetQuery * settings = ast->storage->settings;
            if (!settings)
                ast->storage->set(settings, std::make_shared<ASTSetQuery>());
            settings->is_standalone = false;
            settings->changes.emplace_back("enable_metastore", false);
        }

        if (engine_name == "UniqueMergeTree")
        {
            ASTSetQuery * settings = ast->storage->settings;
            if (!settings)
                ast->storage->set(settings, std::make_shared<ASTSetQuery>());
            settings->is_standalone = false;
            settings->changes.emplace_back("part_writer_flag", true);
            settings->changes.emplace_back("enable_metastore", false);
        }
    }

    std::string create_sql = serializeAST(*ast);
    try
    {
        execute(std::move(create_sql), query_context);
    }
    catch (...)
    {
        LOG_ERROR(getLogger("MockEnvironment"), "Create table {} failed: {}", table, getCurrentExceptionMessage(true));
    }
}

bool MockEnvironment::isPrimaryKey(const QualifiedColumnName & column, ContextPtr context)
{
    StoragePtr table = tryGetLocalTable(column.database, column.table, context);

    if (!table)
        return false;

    auto metadata = table->getInMemoryMetadataCopy();
    std::optional<KeyDescription> primary_key = std::nullopt;
    if (metadata.isPrimaryKeyDefined())
        primary_key = metadata.getPrimaryKey();
    // From CH: By default the primary key is the same as the sorting key (which is specified by the ORDER BY clause).
    // Thus in most cases it is unnecessary to specify a separate PRIMARY KEY clause.
    else if (auto merge_tree = dynamic_pointer_cast<StorageMergeTree>(table); merge_tree && metadata.isSortingKeyDefined())
        primary_key = metadata.getSortingKey();

    if (!primary_key)
        return false;

    const auto & primary_key_columns = primary_key.value().expression->getRequiredColumns();
    return std::find(primary_key_columns.begin(), primary_key_columns.end(), column.column) != primary_key_columns.end();
}

StoragePtr MockEnvironment::tryGetLocalTable(const std::string & database_name, const std::string & table_name, ContextPtr context)
{
    StoragePtr table;

    if (DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name, context))
        table = database->tryGetTable(table_name, context);

    if (auto distributed = dynamic_pointer_cast<StorageDistributed>(table))
        if (auto remote_database = DatabaseCatalog::instance().tryGetDatabase(distributed->getRemoteDatabaseName(), context))
            if (auto remote_table = remote_database->tryGetTable(distributed->getRemoteTableName(), context))
                table = remote_table;

    return table;
}



} // DB
