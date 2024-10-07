#include "DatabasePaimon.h"

#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNSUPPORTED_METHOD;
    extern const int TABLE_IS_DROPPED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DatabasePaimon::DatabasePaimon(
    ContextPtr context_, const String & database_name_, const String & metadata_path_, const ASTStorage * database_engine_define_)
    : DatabaseLakeBase(database_name_, metadata_path_, database_engine_define_)
{
    auto * storage_ast_ptr = static_cast<ASTStorage *>(database_engine_define.get());
    storage_settings = std::make_shared<CnchHiveSettings>(context_->getCnchHiveSettings());
    storage_settings->loadFromQuery(*storage_ast_ptr);
    const String argument_syntax = "{hive|filesystem:HDFS|filesystem:S3, uri, db_name}";
    ASTs engine_args = storage_ast_ptr->engine->arguments->getChildren();
    if (engine_args.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Paimon database require 3 arguments: {}.", argument_syntax);

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context_);

    const String metastore_type = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
    const String uri = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
    database_name_in_paimon = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

    if (metastore_type == "hive")
    {
        catalog_client = PaimonHiveCatalogClient::create(context_, storage_settings, uri);
    }
    else if (metastore_type == "filesystem:HDFS")
    {
        catalog_client = PaimonHDFSCatalogClient::create(context_, storage_settings, uri);
    }
    else if (metastore_type == "filesystem:S3")
    {
        if (storage_settings->region.value.empty() || storage_settings->endpoint.value.empty() || storage_settings->ak_id.value.empty()
            || storage_settings->ak_secret.value.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Storage settings {region, endpoint, ak_id, ak_secret} must be set for S3 filesystem type.");
        }
        catalog_client = PaimonS3CatalogClient::create(context_, storage_settings, uri);
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The first argument of Paimon database must be 'hive|filesystem:HDFS|filesystem:S3', the complete "
            "arguments should be {}.",
            argument_syntax);
    }
}

bool DatabasePaimon::empty() const
{
    return catalog_client->listDatabases().empty();
}

DatabaseTablesIteratorPtr DatabasePaimon::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name)
{
    std::vector<String> all_table_names;
    try
    {
        all_table_names = catalog_client->listTables(database_name_in_paimon);
    }
    catch (Exception &)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw Exception(
            ErrorCodes::UNKNOWN_DATABASE,
            fmt::format(
                "Please drop the Paimon Database {}, because the corresponding database {} in paimon catalog not exists.",
                database_name,
                database_name_in_paimon));
    }
    std::vector<StoragePtr> all_tables;
    all_tables.reserve(all_table_names.size());
    std::vector<String> all_names;
    all_names.reserve(all_table_names.size());
    for (const auto & table_name : all_table_names)
    {
        if (!filter_by_table_name || filter_by_table_name(table_name))
        {
            auto tbl = tryGetTable(table_name, local_context);
            if (tbl)
            {
                all_tables.emplace_back(tbl);
                all_names.emplace_back(table_name);
            }
        }
    }
    return std::make_unique<LakeDatabaseTablesIterator>(database_name_in_paimon, std::move(all_tables), std::move(all_names));
}

bool DatabasePaimon::isTableExist(const String & table_name, ContextPtr /*context*/) const
{
    return catalog_client->isTableExist(database_name_in_paimon, table_name);
}

time_t DatabasePaimon::getObjectMetadataModificationTime(const String & /*table_name*/) const
{
    return {};
}

ASTPtr DatabasePaimon::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    try
    {
        Protos::Paimon::Schema schema = catalog_client->getPaimonSchema(database_name_in_paimon, table_name);
        paimon_utils::PaimonSchemaConverter converter(local_context, storage_settings, schema);
        auto create_query_ast = converter.createQueryAST(catalog_client, getDatabaseName(), database_name_in_paimon, table_name);
        create_query_ast.database = getDatabaseName();
        create_query_ast.table = table_name;

        addCreateQuerySettings(create_query_ast, storage_settings);
        return create_query_ast.clone();
    }
    catch (...)
    {
        if (throw_on_error)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw Exception(ErrorCodes::UNKNOWN_TABLE, fmt::format("Paimon table {}.{} not exists.", database_name_in_paimon, table_name));
        }
        return nullptr;
    }
}

}

#endif
