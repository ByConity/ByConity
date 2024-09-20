#include "Storages/DataLakes/StorageCnchLas.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Protos/lake_models.pb.h>
#include <Storages/DataLakes/ScanInfo/ILakeScanInfo.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/Hive/HivePartition.h>
#include <Storages/Hive/HiveWhereOptimizer.h>
#include <Storages/Hive/Metastore/JNIHiveMetastore.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <Storages/StorageFactory.h>
#include <jni/JNIMetaClient.h>
#include <hudi.pb.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

static constexpr auto LAS_CLASS_FACTORY_CLASS = "org/byconity/las/LasClassFactory";
static constexpr auto LAS_CLASS = "org/byconity/las/LasMetaClient";

StorageCnchLas::StorageCnchLas(
    const StorageID & table_id_,
    const String & hive_metastore_url_,
    const String & hive_db_name_,
    const String & hive_table_name_,
    StorageInMemoryMetadata metadata_,
    ContextPtr context_,
    IMetaClientPtr client,
    std::shared_ptr<CnchHiveSettings> settings_)
    : StorageCnchHive(table_id_, hive_metastore_url_, hive_db_name_, hive_table_name_, std::nullopt, context_, nullptr, settings_)
{
    std::unordered_map<String, String> params = {
        {"database_name", hive_db_name_},
        {"table_name", hive_table_name_},
        {"metastore_uri", hive_metastore_url_},
        {"endpoint", storage_settings->endpoint},
        {"access_key", storage_settings->ak_id},
        {"secret_key", storage_settings->ak_secret},
        {"region", storage_settings->region},
    };

    DB::Protos::HudiMetaClientParams req;
    auto * prop = req.mutable_properties();
    for (const auto & kv : params)
    {
        auto * proto_kv = prop->add_properties();
        proto_kv->set_key(kv.first);
        proto_kv->set_value(kv.second);
    }

    auto metaclient = std::make_shared<JNIMetaClient>(LAS_CLASS_FACTORY_CLASS, LAS_CLASS, req.SerializeAsString());
    if (!client)
    {
        auto cli = std::make_shared<JNIHiveMetastoreClient>(std::move(metaclient), std::move(params));
        jni_meta_client = cli.get();
        client = std::move(cli);
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageLas does not support external catalog");

    setHiveMetaClient(std::move(client));

    Stopwatch watch;
    initialize(metadata_);
    LOG_TRACE(log, "Elapsed {} ms to getTable from metastore", watch.elapsedMilliseconds());
}

PrepareContextResult StorageCnchLas::prepareReadContext(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr & local_context,
    [[maybe_unused]] unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    Stopwatch watch;
    HivePartitions partitions = selectPartitions(local_context, metadata_snapshot, query_info);
    LOG_TRACE(log, "Elapsed {} ms to select {} required partitions", watch.elapsedMilliseconds(), partitions.size());

    /// TODO: This looks very hacky I know
    /// but should be valid, because jni does not allow multi thread access
    auto & read_properties = jni_meta_client->getProperties();
    read_properties["input_format"] = hive_table->sd.inputFormat;
    read_properties["serde"] = hive_table->sd.serdeInfo.serializationLib;
    read_properties["hive_column_names"] = fmt::format("{}", fmt::join(getHiveColumnNames(), ","));
    read_properties["hive_column_types"] = fmt::format("{}", fmt::join(getHiveColumnTypes(), "#"));

    const auto & settings = local_context->getSettingsRef();
    read_properties["buffer_size"] = std::to_string(settings.max_read_buffer_size);

    const auto & hive_table_properties = hive_table->parameters;
    Strings fs_options_props;
    for (const auto & [k, v] : hive_table_properties)
    {
        fs_options_props.emplace_back(fmt::format("{}={}", k, v));
    }
    read_properties["fs_options_props"] = fmt::format("{}", fmt::join(fs_options_props, "#"));

    watch.restart();
    size_t num_splits = local_context->getCurrentWorkerGroup()->getShardsInfo().size() * settings.max_threads;
    LakeScanInfos lake_scan_infos = jni_meta_client->getFilesInPartition(partitions, num_splits, static_cast<size_t>(num_streams));

    LOG_TRACE(log, "Elapsed {} ms to get {} FileSplits", watch.elapsedMilliseconds(), lake_scan_infos.size());
    PrepareContextResult result{.lake_scan_infos = std::move(lake_scan_infos)};
    collectResource(local_context, result);
    return result;
}

std::optional<TableStatistics>
StorageCnchLas::getTableStats([[maybe_unused]] const Strings & columns, [[maybe_unused]] ContextPtr local_context)
{
    return std::nullopt;
}

Strings StorageCnchLas::getHiveColumnTypes() const
{
    Strings res;
    res.reserve(hive_table->sd.cols.size());
    for (const auto & col : hive_table->sd.cols)
        res.push_back(col.type);
    return res;
}

Strings StorageCnchLas::getHiveColumnNames() const
{
    Strings res;
    res.reserve(hive_table->sd.cols.size());
    for (const auto & col : hive_table->sd.cols)
        res.push_back(col.name);
    return res;
}

void registerStorageLas(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_sort_order = true,
        .supports_schema_inference = true,
    };

    factory.registerStorage(
        "CnchLas",
        [](const StorageFactory::Arguments & args) {
            ASTs & engine_args = args.engine_args;

            String hive_metastore_url;
            String hive_database;
            String hive_table;

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

            if (engine_args.size() == 3)
            {
                hive_metastore_url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
                hive_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
                hive_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
            }
            else if (engine_args.size() == 2)
            {
                hive_database = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
                hive_table = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            }
            else
            {
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage CnchHudi require 2 or 3 arguments: {[hive_metastore_url, ] db_name, able_name}.");
            }

            StorageInMemoryMetadata metadata;
            std::shared_ptr<CnchHiveSettings> las_settings = std::make_shared<CnchHiveSettings>(args.getContext()->getCnchLasSettings());
            if (args.storage_def->settings)
            {
                las_settings->loadFromQuery(*args.storage_def);
                metadata.settings_changes = args.storage_def->settings->ptr();
            }

            if (hive_metastore_url.empty())
            {
                hive_metastore_url = las_settings->hive_metastore_url;
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

            return std::make_shared<StorageCnchLas>(
                args.table_id,
                hive_metastore_url,
                hive_database,
                hive_table,
                std::move(metadata),
                args.getContext(),
                args.hive_client,
                las_settings);
        },
        features);
}

}

#endif
