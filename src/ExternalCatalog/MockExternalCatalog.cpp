#include "MockExternalCatalog.h"
#include <Core/UUID.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <common/logger_useful.h>

namespace DB::ExternalCatalog
{
StoragePtr createStorageFromCreateQuery(const String & catalog, const String & create_table_query, ContextPtr _context)
{
    ParserCreateQuery p_create_query;
    auto ast = parseQuery(
        p_create_query, create_table_query, _context->getSettingsRef().max_query_size, _context->getSettingsRef().max_parser_depth);

    auto & create_query = ast->as<ASTCreateQuery &>();
    create_query.catalog = catalog;
    // create_query.uuid =
    // auto engine = std::make_shared<ASTFunction>();
    // engine->name = "CnchHive";
    // engine->arguments = std::make_shared<ASTExpressionList>();
    // engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(create_query.database));
    // engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(create_query.table));
    // create_query.storage->set(create_query.storage->engine, engine);
    // auto storage_def = create_query.storage;


    // std::stringstream ss;
    // create_query.settings_ast->dumpTree(ss);
    // LOG_INFO(log, "query tree" << ss.str());
    auto ret = StorageFactory::instance().get(
        create_query,
        "",
        _context->getQueryContext(),
        _context->getGlobalContext(),
        // InterpreterCreateQuery::getColumnsDescription(*create_query.columns_list->columns, _context, true),
        {},
        InterpreterCreateQuery::getConstraintsDescription(create_query.columns_list->constraints),
        false);
    ret->setCreateTableSql(create_table_query);
    LOG_DEBUG(&Poco::Logger::get("createStorageFromCreateQuery"), "create table from {} ", create_table_query);
    return ret;
}

DB::StoragePtr MockExternalCatalog::getTable(
    [[maybe_unused]] const std::string & db_name, [[maybe_unused]] const std::string & table_name, ContextPtr local_context)
{
    auto uuid = UUIDHelpers::hashUUIDfromString(fmt::format("{}.{}.{}", catalog_name, db_name, table_name));
    std::string create_query = fmt::format(
        "CREATE TABLE {}.{}.{} UUID '{}' (name Nullable(String), value Nullable(Int32), event_date String )"
        "ENGINE = CnchHive('thrift://localhost:9183', 'orc_db', 'orc_tbl')"
        "PARTITION BY event_date",
        name(),
        db_name,
        table_name,
        UUIDHelpers::UUIDToString(uuid));
    LOG_DEBUG(log, "create table with query: " + create_query);
    auto storage_ptr = createStorageFromCreateQuery(name(), create_query, local_context);
    LOG_DEBUG(
        log,
        fmt::format(
            "created mock table {}.{}.{} {}",
            storage_ptr->getStorageID().catalog_name,
            storage_ptr->getStorageID().database_name,
            storage_ptr->getStorageID().table_name,
            storage_ptr->getInMemoryMetadata().getColumns().toString()));
    return storage_ptr;
}

UUID MockExternalCatalog::getTableUUID(const std::string & db_name, const std::string & table_name)
{
    return UUIDHelpers::hashUUIDfromString(fmt::format("{}.{}.{}", catalog_name, db_name, table_name));
}
}
