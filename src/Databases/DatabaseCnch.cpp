#include <Databases/DatabaseCnch.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Catalog/Catalog.h>

namespace DB
{

DatabaseCnch::DatabaseCnch(const String & name_, UUID uuid, ContextPtr local_context)
    : IDatabase(name_), WithContext(local_context->getGlobalContext())
    , db_uuid(uuid)
    , log(&Poco::Logger::get("DatabaseCnch (" + name_ + ")"))
{
    LOG_DEBUG(log, "Create database {} in query {}", database_name, local_context->getCurrentQueryId());
}

void DatabaseCnch::createTable(
    ContextPtr local_context,
    const String & table_name,
    const StoragePtr & /*table*/,
    const ASTPtr & /*query*/)
{
    LOG_DEBUG(log, "Create table {} in query {}", table_name, local_context->getCurrentQueryId());
}

void DatabaseCnch::dropTable(
    ContextPtr local_context,
    const String & table_name,
    bool no_delay)
{
    LOG_DEBUG(log, "Drop table {} in query {}, no delay {}"
         , table_name, local_context->getCurrentQueryId(), no_delay);
}

void DatabaseCnch::drop(ContextPtr local_context)
{
    LOG_DEBUG(log, "Drop database {} in query {}", database_name , local_context->getCurrentQueryId());
}

ASTPtr DatabaseCnch::getCreateDatabaseQuery() const
{
    auto settings = getContext()->getSettingsRef();
    String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName()) + " ENGINE = Cnch";
    ParserCreateQuery parser(settings.dialect_type);
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);
    return ast;
}

bool DatabaseCnch::isTableExist(const String & /*name*/, ContextPtr /*local_context*/) const
{
    /// TODO update timestamp
    #if 0
    return local_context->getCnchCatalog()->isTableExists(getDatabaseName(), name, TxnTimestamp::maxTS());
    #endif
    return false;
}

StoragePtr DatabaseCnch::tryGetTable(const String & /*name*/, ContextPtr /*local_context*/) const
{
    /// TODO update timestamp
    StoragePtr res{nullptr};
#if 0
    try
    {
        res = local_context->getCnchCatalog()->getTable(*local_context, getDatabaseName(), name, TxnTimestamp::maxTS());
    }
    catch (Exception & e)
    {
        // Only report unexpected exception.
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw e;
    }
#endif
    return res;
}

DatabaseTablesIteratorPtr DatabaseCnch::getTablesIterator(ContextPtr /*local_context*/, const FilterByNameFunction & filter_by_table_name)
{
    Tables tables;
#if 0
    Strings names = local_context->getCnchCatalog()->getTablesInDB(getDatabaseName());
    std::transform(names.begin(), names.end(), std::inserter(tables, tables.end()),
        [this, & local_context] (const String & name)
        {
            StoragePtr storage = tryGetTable(name, local_context);
            if (!storage)
                throw Exception("Can't get storage for table " + name, ErrorCodes::UNKNOWN_TABLE);

            return std::make_pair(name, std::move(storage));
        }
    );
#endif

    if (!filter_by_table_name)
        return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);

    Tables filtered_tables;
    for (const auto & [table_name, storage] : tables)
        if (filter_by_table_name(table_name))
            filtered_tables.emplace(table_name, storage);

    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(filtered_tables), database_name);
}

bool DatabaseCnch::empty() const
{
    #if 0
    Strings tables = getContext()->getCnchCatalog()->getTablesInDB(getDatabaseName());
    return tables.empty();
    #endif
    return true;
}

}
