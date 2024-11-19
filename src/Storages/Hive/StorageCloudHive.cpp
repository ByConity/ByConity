#include <atomic>
#include <Storages/Hive/StorageCloudHive.h>
#if USE_HIVE

#include <DataStreams/narrowBlockInputStreams.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/DataLakes/Source/LakeUnifiedSource.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/Hive/HiveVirtualColumns.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <common/logger_useful.h>
#include <common/scope_guard_safe.h>

using DB::Context;

namespace DB
{
namespace ErrorCodes
{
}

StorageCloudHive::StorageCloudHive(
    StorageID table_id_, const StorageInMemoryMetadata & metadata, ContextPtr context_, const std::shared_ptr<CnchHiveSettings> & settings_)
    : IStorage(table_id_), WithContext(context_->getGlobalContext()), storage_settings(settings_)
{
    setInMemoryMetadata(metadata);
}

LakeScanInfos StorageCloudHive::filterLakeScanInfosByIntermediateResultCache(
    SelectQueryInfo & query_info, ContextPtr query_context, LakeScanInfos & lake_scan_infos_)
{
    auto cache = query_context->getIntermediateResultCache();
    if (!cache)
        return lake_scan_infos_;

    TableScanCacheInfo cache_info = getTableScanCacheInfo(query_info);

    // check enable result cache
    if (cache_info.getStatus() == TableScanCacheInfo::Status::NO_CACHE)
        return lake_scan_infos_;

    if (cache_info.getStatus() == TableScanCacheInfo::Status::CACHE_DEPENDENT_TABLE)
    {
        return lake_scan_infos_;
    }
    auto s_id = getStorageID();

    assert(cache_info.getStatus() == TableScanCacheInfo::Status::CACHE_TABLE);
    LakeScanInfos new_lake_scan_infos;
    cache_holder = cache->createCacheHolder(query_context, cache_info.getDigest(), s_id, lake_scan_infos_, new_lake_scan_infos);
    return new_lake_scan_infos;
}

Pipe StorageCloudHive::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams_)
{
    const auto & settings_ref = local_context->getSettingsRef();
    const size_t max_num_streams = std::min(static_cast<size_t>(num_streams_), static_cast<size_t>(settings_ref.max_threads));
    size_t num_streams = max_num_streams;
    const size_t max_threads = std::max(static_cast<size_t>(settings_ref.max_parsing_threads), max_num_streams);

    LakeScanInfos lake_scan_infos_before_filter = getLakeScanInfos();
    LakeScanInfos lake_scan_infos_after_filter
        = filterLakeScanInfosByIntermediateResultCache(query_info, local_context, lake_scan_infos_before_filter);

    num_streams = std::min(static_cast<size_t>(num_streams), lake_scan_infos_after_filter.size());

    LOG_DEBUG(
        log,
        "Reading {} files in {} streams with {} threads, disk_cache mode {}",
        lake_scan_infos_after_filter.size(),
        num_streams,
        max_threads,
        local_context->getSettingsRef().disk_cache_mode.toString());

    Pipes pipes;
    pipes.reserve(num_streams);

    auto shared_pool = std::make_shared<SharedParsingThreadPool>(max_threads, num_streams);

    auto block_info = std::make_shared<LakeUnifiedSource::BlockInfo>(
        storage_snapshot->getSampleBlockForColumns(column_names), storage_snapshot->metadata);
    auto allocator = std::make_shared<LakeUnifiedSource::Allocator>(std::move(lake_scan_infos_after_filter));

    auto query_info_ptr = std::make_shared<SelectQueryInfo>(query_info);

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(LakeUnifiedSource::create(local_context, max_block_size, block_info, allocator, query_info_ptr, shared_pool));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    narrowPipe(pipe, num_streams);
    if (cache_holder)
        pipe.addCacheHolder(cache_holder);

    size_t output_ports = pipe.numOutputPorts();
    if (output_ports > 0 && output_ports < max_num_streams && !settings_ref.input_format_parquet_preserve_order)
        pipe.resize(max_num_streams);

    return pipe;
}

NamesAndTypesList StorageCloudHive::getVirtuals() const
{
    return getHiveVirtuals();
}

void StorageCloudHive::loadLakeScanInfos(const LakeScanInfos & lake_scan_infos_)
{
    lake_scan_infos = lake_scan_infos_;
    LOG_DEBUG(log, "Loaded data parts {} items", lake_scan_infos.size());
}

void registerStorageCloudHive(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
    };

    factory.registerStorage(
        "CloudHive",
        [](const StorageFactory::Arguments & args) {
            StorageInMemoryMetadata metadata;
            std::shared_ptr<CnchHiveSettings> settings = std::make_shared<CnchHiveSettings>(args.getContext()->getCnchHiveSettings());
            if (args.storage_def->settings)
            {
                settings->loadFromQuery(*args.storage_def);
                metadata.settings_changes = args.storage_def->settings->ptr();
            }

            metadata.setColumns(args.columns);

            if (args.storage_def->partition_by)
            {
                ASTPtr partition_by_key;
                partition_by_key = args.storage_def->partition_by->ptr();
                metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.getContext());
            }

            return StorageCloudHive::create(args.table_id, metadata, args.getContext(), settings);
        },
        features);
}

}

#endif
