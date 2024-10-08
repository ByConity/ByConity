#include "Storages/DataLakes/StorageCnchHudi.h"
#if USE_HIVE

#include <hive_metastore_types.h>
#include "Interpreters/evaluateConstantExpression.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ASTLiteral.h"
#include "Storages/DataLakes/HudiDirectoryLister.h"
#include "Storages/DataLakes/HudiMorDirectoryLister.h"
#include "Storages/Hive/CnchHiveSettings.h"
#include "Storages/Hive/DirectoryLister.h"
#include "Storages/StorageFactory.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_FORMAT;
}

StorageCnchHudi::StorageCnchHudi(
    const StorageID & table_id_,
    const String & hive_metastore_url_,
    const String & hive_db_name_,
    const String & hive_table_name_,
    StorageInMemoryMetadata metadata_,
    ContextPtr context_,
    std::shared_ptr<CnchHiveSettings> settings_,
    IMetaClientPtr client_from_catalog)
    : StorageCnchHive(
        table_id_,
        hive_metastore_url_,
        hive_db_name_,
        hive_table_name_,
        metadata_,
        context_,
        client_from_catalog,
        settings_)
{
}

std::shared_ptr<IDirectoryLister> StorageCnchHudi::getDirectoryLister(ContextPtr local_context)
{
    auto disk = HiveUtil::getDiskFromURI(hive_table->sd.location, local_context, *storage_settings);
    const auto & input_format = hive_table->sd.inputFormat;
    if (input_format == "org.apache.hudi.hadoop.HoodieParquetInputFormat")
    {
        return std::make_shared<HudiCowDirectoryLister>(disk);
    }
#if USE_JAVA_EXTENSIONS
    else if (input_format == "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat")
    {
        return std::make_shared<HudiMorDirectoryLister>(disk, hive_table->sd.location, *this);
    }
#endif
    else
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown hudi format {}", input_format);
}

Strings StorageCnchHudi::getHiveColumnTypes() const
{
    Strings res;
    res.reserve(hive_table->sd.cols.size());
    for (const auto & col : hive_table->sd.cols)
        res.push_back(col.type);
    return res;
}

Strings StorageCnchHudi::getHiveColumnNames() const
{
    Strings res;
    res.reserve(hive_table->sd.cols.size());
    for (const auto & col : hive_table->sd.cols)
        res.push_back(col.name);
    return res;
}

void registerStorageHudi(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_sort_order = true,
        .supports_schema_inference = true,
    };

    factory.registerStorage("CnchHudi", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        if (engine_args.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage CnchHudi require 3 arguments: hive_metastore_url, hudi_db_name and hudi_table_name.");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        String hive_metastore_url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        String hive_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        String hive_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        StorageInMemoryMetadata metadata;
        std::shared_ptr<CnchHiveSettings> hive_settings = std::make_shared<CnchHiveSettings>(args.getContext()->getCnchHiveSettings());
        if (args.storage_def->settings)
        {
            hive_settings->loadFromQuery(*args.storage_def);
            metadata.settings_changes = args.storage_def->settings->ptr();
        }

        if (!args.columns.empty())
            metadata.setColumns(args.columns);

        metadata.setComment(args.comment);

        if (args.storage_def->partition_by)
        {
            ASTPtr partition_by_key;
            partition_by_key = args.storage_def->partition_by->ptr();
            metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.getContext());
        }

        return std::make_shared<StorageCnchHudi>(
            args.table_id,
            hive_metastore_url,
            hive_database,
            hive_table,
            std::move(metadata),
            args.getContext(),
            hive_settings,
            args.hive_client);
    },
    features);
}

}

#endif
