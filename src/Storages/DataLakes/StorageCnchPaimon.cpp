#include "StorageCnchPaimon.h"

#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <CloudServices/CnchServerResource.h>
#include <DataStreams/narrowBlockInputStreams.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/PushFilterToStorage.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Optimizer/PredicateUtils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/DataLakes/ScanInfo/FileScanInfo.h>
#include <Storages/DataLakes/ScanInfo/PaimonJNIScanInfo.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/Hive/DirectoryLister.h>
#include <Storages/Hive/HiveSchemaConverter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/StorageFactory.h>
#include <jni/JNIMetaClient.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StorageCnchPaimon::StorageCnchPaimon(
    const ContextPtr & context_,
    const StorageID & table_id_,
    const String & database_,
    const String & table_,
    CnchHiveSettingsPtr storage_settings_,
    StorageInMemoryMetadata metadata_,
    PaimonCatalogClientPtr catalog_client_)
    : StorageCnchLakeBase(table_id_, database_, table_, context_, storage_settings_), catalog_client(catalog_client_)
{
    catalog_client->convertMetadataIfNecessary(db_name, table_name, metadata_);
    setInMemoryMetadata(metadata_);
}

ASTPtr StorageCnchPaimon::applyFilter(
    ASTPtr query_filter, SelectQueryInfo & query_info, ContextPtr query_context, PlanNodeStatisticsPtr stats) const
{
    const auto & settings = query_context->getSettingsRef();
    auto * select_query = query_info.getSelectQuery();
    if (settings.optimize_move_to_prewhere && query_filter && !select_query->prewhere()
        && (!select_query->final() || settings.optimize_move_to_prewhere_if_final))
    {
        PushFilterToStorage push_filter_to_storage(shared_from_this(), query_context);

        // Just discard partition filter
        auto [_, non_partition_conjuncts] = push_filter_to_storage.extractPartitionFilter(query_filter, true);
        auto non_partition_filter = PredicateUtils::combineConjuncts(non_partition_conjuncts);

        if (!PredicateUtils::isTruePredicate(non_partition_filter))
        {
            // Set all non-partition filters as prewhere
            select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, std::move(non_partition_filter));
            LOG_DEBUG(log, "Push filter to storage, prewhere: {}", queryToString(select_query->prewhere()));
        }
    }

    // We deliberately do not change the query_filter. Because the pushed filter may not work in the remote storage,
    // and the initial filter is still needed to guarantee the correctness of the result.
    IStorage::applyFilter(query_filter, query_info, query_context, stats);
    return query_filter;
}

PrepareContextResult StorageCnchPaimon::prepareReadContext(
    const Names & column_names,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr & query_context,
    unsigned /*num_streams*/)
{
    std::optional<String> predicate = std::nullopt;
    auto where = query_info.query->as<ASTSelectQuery &>().where();
    if (where)
    {
        predicate = paimon_utils::Predicate2RPNConverter::convert(where);
        if (predicate.has_value())
            LOG_DEBUG(log, "rpn_predicate: {}", predicate.value());
        else
            LOG_ERROR(log, "failed to convert filter to RPN, filter={}", queryToString(where));
    }

    auto scan_info
        = catalog_client->getScanInfo(db_name, table_name, column_names, predicate, query_context->getSettingsRef().force_paimon_use_jni);
    LOG_DEBUG(log, "ScanInfo, encoded_splits size: {}, raw_files size: {}", scan_info.encoded_splits.size(), scan_info.raw_files.size());


    LakeScanInfos lake_scan_infos;
    size_t id = 0;
    for (auto && split : scan_info.encoded_splits)
    {
        lake_scan_infos.push_back(
            PaimonJNIScanInfo::create(id++, scan_info.encoded_table, scan_info.encoded_predicate, std::vector<String>{std::move(split)}));
    }
    for (auto && raw_file_json_str : scan_info.raw_files)
    {
        Poco::JSON::Parser parser;
        Poco::JSON::Object::Ptr raw_file = parser.parse(raw_file_json_str).extract<Poco::JSON::Object::Ptr>();
        const String & path = raw_file->getValue<String>("path");
        const size_t & size = raw_file->getValue<size_t>("size");
        const String & format = raw_file->getValue<String>("format");
        Poco::URI uri(path);

        if (format == "ORC")
        {
            lake_scan_infos.push_back(FileScanInfo::create(
                ILakeScanInfo::StorageType::Paimon, FileScanInfo::FormatType::ORC, uri.toString(), size, std::nullopt));
        }
        else if (format == "Parquet")
        {
            lake_scan_infos.push_back(FileScanInfo::create(
                ILakeScanInfo::StorageType::Paimon, FileScanInfo::FormatType::PARQUET, uri.toString(), size, std::nullopt));
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported file format: {}", format);
        }
    }

    PrepareContextResult result{.lake_scan_infos = std::move(lake_scan_infos)};
    collectResource(query_context, result);
    return result;
}

void registerStorageCnchPaimon(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
        .supports_schema_inference = true,
    };

    auto fn = [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        const String argument_syntax = "{hive|filesystem:HDFS|filesystem:S3, uri, db_name, table_name}";
        if (engine_args.size() != 4)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "StorageCnchPaimon require 4 arguments: {}.", argument_syntax);
        }

        const String metastore_type = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        const String uri = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String database = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String table = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();

        StorageInMemoryMetadata metadata;
        metadata.setComment(args.comment);
        metadata.setConstraints(args.constraints);
        if (args.storage_def->settings)
            metadata.settings_changes = args.storage_def->settings->ptr();
        if (!args.columns.empty())
            metadata.setColumns(args.columns);
        if (args.storage_def->partition_by)
        {
            if (metadata.columns.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition by clause is not allowed when using auto schema.");
            ASTPtr partition_by_key = args.storage_def->partition_by->ptr();
            metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.getContext());
        }

        std::shared_ptr<CnchHiveSettings> storage_settings = std::make_shared<CnchHiveSettings>(args.getContext()->getCnchHiveSettings());
        storage_settings->loadFromQuery(*args.storage_def);

        if (metastore_type == "hive")
        {
            return std::make_shared<StorageCnchPaimon>(
                args.getContext(),
                args.table_id,
                database,
                table,
                storage_settings,
                std::move(metadata),
                PaimonHiveCatalogClient::create(args.getContext(), storage_settings, uri));
        }
        else if (metastore_type == "filesystem:HDFS")
        {
            return std::make_shared<StorageCnchPaimon>(
                args.getContext(),
                args.table_id,
                database,
                table,
                storage_settings,
                std::move(metadata),
                PaimonHDFSCatalogClient::create(args.getContext(), storage_settings, uri));
        }
        else if (metastore_type == "filesystem:S3")
        {
            if (storage_settings->region.value.empty() || storage_settings->endpoint.value.empty() || storage_settings->ak_id.value.empty()
                || storage_settings->ak_secret.value.empty())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Storage settings {region, endpoint, ak_id, ak_secret} must be set for S3 filesystem type.");
            }
            return std::make_shared<StorageCnchPaimon>(
                args.getContext(),
                args.table_id,
                database,
                table,
                storage_settings,
                std::move(metadata),
                PaimonS3CatalogClient::create(args.getContext(), storage_settings, uri));
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The first argument of StorageCnchPaimon must be 'hive|filesystem:HDFS|filesystem:S3', the complete "
                "arguments should be {}.",
                argument_syntax);
        }
    };

    factory.registerStorage("CnchPaimon", fn, features);
}
}

#endif
