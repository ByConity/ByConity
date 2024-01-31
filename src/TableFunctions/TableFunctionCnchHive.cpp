#include "TableFunctions/TableFunctionCnchHive.h"
#include "Databases/IDatabase.h"

#if USE_HIVE
#include <memory>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include "Storages/Hive/StorageCnchHive.h"
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionCnchHive::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
{
    ASTs & args_func = ast_function_->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "The signature of function {} is:\n - hive_url, hive_database, hive_table",
                        getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    hive_metastore_url = args[0]->as<const ASTLiteral &>().value.safeGet<String>();
    hive_database_name = args[1]->as<const ASTLiteral &>().value.safeGet<String>();
    hive_table_name = args[2]->as<const ASTLiteral &>().value.safeGet<String>();

    auto hive_settings = std::make_shared<CnchHiveSettings>(context_->getCnchHiveSettings());
    storage = StorageCnchHive::create(
        StorageID(getDatabaseName(), getName()),
        hive_metastore_url,
        hive_database_name,
        hive_table_name,
        StorageInMemoryMetadata{},
        context_,
        nullptr,
        hive_settings);

    storage->startup();
}

ColumnsDescription TableFunctionCnchHive::getActualTableStructure(ContextPtr /*context_*/) const
{
    return storage->getInMemoryMetadataPtr()->columns;
}

StoragePtr TableFunctionCnchHive::executeImpl(
    const ASTPtr & /*ast_function_*/,
    ContextPtr /*context_*/,
    const std::string & table_name_,
    ColumnsDescription /*cached_columns_*/) const
{
    StorageID storage_id = storage->getStorageID();
    storage_id.table_name = table_name_;
    storage->renameInMemory(storage_id);
    return storage;
}

void registerTableFunctionCnchHive(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCnchHive>();
}

}

#endif
