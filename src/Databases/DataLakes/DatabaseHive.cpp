#include "DatabaseHive.h"

#if USE_HIVE
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/Hive/HiveSchemaConverter.h>
#include <Storages/StorageFactory.h>
#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Common/parseAddress.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNSUPPORTED_METHOD;
    extern const int TABLE_IS_DROPPED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DatabaseHive::DatabaseHive(
    ContextPtr context_, const String & database_name_, const String & metadata_path_, const ASTStorage * database_engine_define_)
    : DatabaseLakeBase(database_name_, metadata_path_, database_engine_define_)
{
    auto * storage_ast_ptr = static_cast<ASTStorage *>(database_engine_define.get());
    // loadFromQuery will render args.storage_def->settings not null;
    storage_settings = std::make_shared<CnchHiveSettings>(context_->getCnchHiveSettings());
    storage_settings->loadFromQuery(*storage_ast_ptr);
    ASTs engine_args = storage_ast_ptr->engine->arguments->getChildren();
    if (engine_args.size() != 2)
        throw Exception(
            "Hive database requires 2 parameters: {hive metadata server url, hive database}", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context_);

    hive_metastore_url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
    database_name_in_hive = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
    metastore_client = HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url, storage_settings);
}

bool DatabaseHive::empty() const
{
    return metastore_client->getAllTables(database_name_in_hive).empty();
}


DatabaseTablesIteratorPtr DatabaseHive::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name)
{
    auto all_table_name = metastore_client->getAllTables(database_name_in_hive);
    std::vector<StoragePtr> all_tables;
    all_tables.reserve(all_table_name.size());
    std::vector<String> all_names;
    all_names.reserve(all_table_name.size());
    for (const auto & name : all_table_name)
    {
        if (!filter_by_table_name || filter_by_table_name(name))
        {
            auto tbl = tryGetTable(name, local_context);
            if (tbl)
            {
                all_tables.emplace_back(tbl);
                all_names.emplace_back(name);
            }
        }
    }
    return std::make_unique<LakeDatabaseTablesIterator>(database_name_in_hive, std::move(all_tables), std::move(all_names));
}

bool DatabaseHive::isTableExist(const String & name, ContextPtr) const
{
    return metastore_client->isTableExist(database_name_in_hive, name);
}

UUID getTableUUID(const std::string & db_name, const std::string & table_name)
{
    return UUIDHelpers::hashUUIDfromString(fmt::format("{}.{}", db_name, table_name));
}

ASTPtr DatabaseHive::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    auto hive_table = metastore_client->getTable(database_name_in_hive, table_name);

    auto error_handler = [&](int ec, const String & msg) -> ASTPtr {
        if (throw_on_error)
        {
            throw Exception(ec, msg);
        }
        return nullptr;
    };

    if (!hive_table)
        return error_handler(ErrorCodes::UNKNOWN_TABLE, "Table " + table_name + " not found in hive database " + database_name_in_hive);
    HiveSchemaConverter converter(local_context, hive_table);
    auto create_query_ast = converter.createQueryAST(hive_metastore_url);
    create_query_ast.database = getDatabaseName();
    create_query_ast.table = hive_table->tableName;

    addCreateQuerySettings(create_query_ast, storage_settings);
    return create_query_ast.clone();
}

time_t DatabaseHive::getObjectMetadataModificationTime(const String & table_name) const
{
    auto hive_table = metastore_client->getTable(database_name_in_hive, table_name);
    return static_cast<time_t>(hive_table->createTime);
}

}

#endif
