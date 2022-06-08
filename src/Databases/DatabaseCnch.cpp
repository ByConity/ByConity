#include <Databases/DatabaseCnch.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

DatabaseCnch::DatabaseCnch(const String & name, ContextPtr local_context, UUID uuid_)
    : DatabaseWithOwnTablesBase{name, "DatabaseCnch (" + name + ")", local_context}, db_uuid(std::move(uuid_))
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

}
