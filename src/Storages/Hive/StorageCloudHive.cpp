#include <Interpreters/Context.h>
#include <Storages/Hive/StorageCloudHive.h>
#if USE_HIVE

#include "DataTypes/DataTypeString.h"
#include "DataStreams/narrowBlockInputStreams.h"
#include "Interpreters/ActionsDAG.h"
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
    const StorageMetadataPtr & metadata_snapshot,
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
    selectFiles(local_context, metadata_snapshot, query_info, hive_files, num_streams);

    Pipes pipes;
    auto block_info = std::make_shared<StorageHiveSource::BlockInfo>(
        metadata_snapshot->getSampleBlockForColumns(real_columns), need_path_colum, need_file_column, metadata_snapshot->getPartitionKey());
    auto allocator = std::make_shared<StorageHiveSource::Allocator>(std::move(hive_files));
    if (block_info->to_read.columns() == 0)
        allocator->allow_allocate_by_slice = false;

    LOG_DEBUG(log, "read with {} streams, disk_cache mode {}", num_streams, local_context->getSettingsRef().disk_cache_mode.toString());
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageHiveSource>(
            local_context,
            block_info,
            allocator
        ));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    narrowPipe(pipe, num_streams);
    return pipe;
}

StorageCloudHive::MinMaxDescription StorageCloudHive::getMinMaxIndex(const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    /// columns not in partition key column can be used
    NamesAndTypesList required_cols = metadata_snapshot->getColumns().getAllPhysical();
    NameSet partition_cols;
    if (metadata_snapshot->hasPartitionKey())
    {
        const auto & col_names = metadata_snapshot->getPartitionKey().column_names;
        partition_cols = NameSet{col_names.begin(), col_names.end()};
    }
    required_cols.remove_if([&] (const auto & name_type) {
        return partition_cols.contains(name_type.name);
    });

    MinMaxDescription description;
    description.expression = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(required_cols), ExpressionActionsSettings::fromContext(local_context));
    description.columns = std::move(required_cols);
    return description;
}

void StorageCloudHive::selectFiles(
    ContextPtr local_context,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    HiveFiles & hive_files,
    unsigned num_streams)
{
    const auto & settings = local_context->getSettingsRef();
    std::optional<KeyCondition> condition;
    StorageCloudHive::MinMaxDescription description;
    if (settings.use_hive_split_level_filter)
    {
        description = getMinMaxIndex(metadata_snapshot, local_context);
        condition.emplace(query_info, local_context, description.columns.getNames(), description.expression);
    }

    if (!(condition && !condition->alwaysUnknownOrTrue()))
    {
        /// minmax index is not useful
        return;
    }

    auto process_hive_file = [&] (IHiveFile & hive_file)
    {
        auto supported_features = hive_file.getFeatures();

        if (supported_features.support_file_minmax_index && condition)
        {
            /// TODO:
            /// hive_file.loadFileMinmaxIndex();
        }

        if (supported_features.support_file_splits && condition)
        {
            std::vector<bool> skip_splits;
            hive_file.loadSplitMinMaxIndex(description.columns);
            const auto & minmax_idxes = hive_file.getSplitMinMaxIndex();
            auto types = description.columns.getTypes();
            for (size_t i = 0; i < minmax_idxes.size(); ++i)
            {
                if (!condition->checkInHyperrectangle(minmax_idxes[i]->hyperrectangle, types).can_be_true)
                {
                    skip_splits[i] = true;
                    LOG_TRACE(log, "Skip hive file {} by minmax index {}",
                        hive_file.file_path, hive_file.describeMinMaxIndex(description.columns));
                }
            }
            hive_file.setSkipSplits(std::move(skip_splits));
        }
    };

    size_t num_threads = std::min(static_cast<size_t>(num_streams), files.size());
    if (num_threads <= 1)
    {
        for (auto & file : hive_files)
            process_hive_file(*file);
    }
    else
    {
        ThreadPool pool(num_streams);
        for (auto & file : hive_files)
        {
            pool.scheduleOrThrowOnError([&, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachQueryIfNotDetached(););
                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                process_hive_file(*file);
            });
        }
        pool.wait();
    }
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
        std::shared_ptr<CnchHiveSettings> settings = std::make_shared<CnchHiveSettings>();
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
