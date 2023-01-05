#pragma once
#include <Interpreters/Context.h>
namespace DB
{

bool createDatabaseInCatalog(
    const ContextPtr & global_context,
    const String & database_name,
    Poco::Logger * logger);

/// Detects change in table schema. Does not support modification of primary/partition keys
String makeAlterColumnQuery(
    const String & database,
    const String & table,
    const Block & expected,
    const Block & actual);

bool createCnchTable(
    ContextPtr global_context,
    const String & database,
    const String & table,
    ASTPtr & create_query_ast,
    Poco::Logger * logger);

bool prepareCnchTable(
    ContextPtr global_context,
    const String & database,
    const String & table,
    ASTPtr & create_query_ast,
    Poco::Logger * logger);

bool syncTableSchema(
    ContextPtr global_context,
    const String & database,
    const String & table,
    const Block & expected_block,
    Poco::Logger * logger);

bool createView(
    ContextPtr global_context,
    const String & database,
    const String & table,
    Poco::Logger * logger);

}/// end namespace
