#include <atomic>
#include <Storages/Hive/StorageCloudHive.h>
#if USE_HIVE

#include "DataTypes/DataTypeString.h"
#include "DataStreams/narrowBlockInputStreams.h"
#include "Interpreters/ActionsDAG.h"
#include "Interpreters/Context.h"
#include "Interpreters/ExpressionActionsSettings.h"
#include "Parsers/ASTCreateQuery.h"
#include "Storages/Hive/CnchHiveSettings.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "Storages/StorageFactory.h"
#include "Storages/Hive/StorageHiveSource.h"
#include "common/logger_useful.h"
#include "common/scope_guard_safe.h"

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

Pipe StorageCloudHive::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &  query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum  /*processed_stage*/,
    [[maybe_unused]] size_t max_block_size,
    unsigned num_streams)
{
    bool need_path_colum = false;
    bool need_file_column = false;

    Names real_columns;
    for (const auto & column : column_names)
    {
        if (column == "_path")
            need_path_colum = true;
        else if (column == "_file")
            need_file_column = true;
        else
            real_columns.push_back(column);
    }

    HiveFiles hive_files = getHiveFiles();
    selectFiles(local_context, storage_snapshot->metadata, query_info, hive_files, num_streams);

    Pipes pipes;
    auto block_info = std::make_shared<StorageHiveSource::BlockInfo>(
        storage_snapshot->getSampleBlockForColumns(real_columns), need_path_colum, need_file_column, storage_snapshot->metadata);
    auto allocator = std::make_shared<StorageHiveSource::Allocator>(std::move(hive_files));

    LOG_DEBUG(log, "read with {} streams, disk_cache mode {}", num_streams, local_context->getSettingsRef().disk_cache_mode.toString());
    auto query_info_ptr = std::make_shared<SelectQueryInfo>(query_info);

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageHiveSource>(
            local_context,
            block_info,
            allocator,
            query_info_ptr
        ));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    narrowPipe(pipe, num_streams);
    return pipe;
}

void StorageCloudHive::selectFiles(
    ContextPtr,
    const StorageMetadataPtr &,
    const SelectQueryInfo &,
    HiveFiles & hive_files,
    unsigned /*num_streams*/)
{
    LOG_DEBUG(log, "Selected {} hive files", hive_files.size());
}

NamesAndTypesList StorageCloudHive::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()}
    };
}

void StorageCloudHive::loadHiveFiles(const HiveFiles & hive_files)
{
    files = hive_files;
    LOG_DEBUG(log, "Loaded data parts {} items", files.size());
}

void registerStorageCloudHive(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
    };

    factory.registerStorage("CloudHive", [](const StorageFactory::Arguments & args)
    {
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
