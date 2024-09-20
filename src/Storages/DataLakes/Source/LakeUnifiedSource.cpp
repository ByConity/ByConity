#include "LakeUnifiedSource.h"

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/DataLakes/ScanInfo/FileScanInfo.h>
#include <Storages/Hive/HivePartition.h>
#include <Storages/Hive/HiveVirtualColumns.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_STORAGE;
}

LakeUnifiedSource::BlockInfo::BlockInfo(const Block & header_, const StorageMetadataPtr & metadata_)
    : header(header_), physical_header(header_), metadata(metadata_)
{
    if (metadata->hasPartitionKey())
    {
        Names names = metadata->getColumnsRequiredForPartitionKey();
        NameSet partition_column_names(names.begin(), names.end());

        const auto & partition_key = metadata->getPartitionKey();
        for (size_t i = 0; i < partition_key.column_names.size(); ++i)
        {
            const auto & name = partition_key.column_names[i];
            if (physical_header.has(name))
                physical_header.erase(name);
            partition_name_to_index[name] = i;
        }
    }

    eraseHiveVirtuals(physical_header);
}

Block LakeUnifiedSource::BlockInfo::getHeader() const
{
    return header;
}

LakeUnifiedSource::Allocator::Allocator(LakeScanInfos && lake_scan_infos_) : lake_scan_infos(std::move(lake_scan_infos_))
{
}

LakeScanInfoPtr LakeUnifiedSource::Allocator::next() const
{
    size_t idx = unallocated.fetch_add(1);
    if (idx >= lake_scan_infos.size())
        return nullptr;

    return lake_scan_infos[idx];
}

LakeUnifiedSource::LakeUnifiedSource(
    ContextPtr context_,
    const size_t max_block_size_,
    BlockInfoPtr info_,
    AllocatorPtr allocator_,
    const std::shared_ptr<SelectQueryInfo> & query_info_,
    const SharedParsingThreadPoolPtr & shared_pool_)
    : SourceWithProgress(info_->getHeader())
    , WithContext(context_)
    , block_info(info_)
    , allocator(allocator_)
    , shared_pool(shared_pool_)
    , need_only_count(
          (query_info_->optimize_trivial_count && context_->getSettingsRef().optimize_trivial_count_query) || !info_->physical_header)
{
    /// Update some hive settings
    FormatSettings format_settings = getFormatSettings(context_);
    const auto & settings_ref = context_->getSettingsRef();
    format_settings.parquet.allow_missing_columns = settings_ref.hive_allow_missing_columns;
    format_settings.parquet.case_insensitive_column_matching = settings_ref.hive_case_insensitive_column_matching;
    format_settings.parquet.use_native_reader = settings_ref.hive_use_native_reader;

    format_settings.orc.allow_missing_columns = settings_ref.hive_allow_missing_columns;
    format_settings.orc.case_insensitive_column_matching = settings_ref.hive_case_insensitive_column_matching;
    format_settings.orc.use_fast_decoder = settings_ref.hive_use_native_reader ? 2 : 0;

    read_params = std::make_shared<ILakeScanInfo::ReadParams>(ILakeScanInfo::ReadParams{
        .max_block_size = max_block_size_,
        .format_settings = std::move(format_settings),
        .context = context_,
        .read_settings = context_->getReadSettings(),
        .query_info = query_info_,
        .shared_pool = shared_pool});
}

LakeUnifiedSource::~LakeUnifiedSource() = default;

void LakeUnifiedSource::prepareReader()
{
    if (reader)
        return;

    lake_scan_info = allocator->next();
    if (!lake_scan_info)
        return;

    pipeline = std::make_unique<QueryPipeline>();
    data_source = getSource();

    pipeline->init(Pipe(data_source));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    auto input_format = std::dynamic_pointer_cast<IInputFormat>(data_source);
    if (input_format && need_only_count)
    {
        input_format->needOnlyCount();
    }

    if (read_params->query_info && read_params->query_info->prewhere_info && input_format && !input_format->supportsPrewhere())
    {
        const auto & query_info = *read_params->query_info;
        auto actions_settings = ExpressionActionsSettings::fromContext(getContext());
        if (query_info.prewhere_info->alias_actions)
        {
            pipeline->addSimpleTransform([&](const Block & header) {
                return std::make_shared<ExpressionTransform>(
                    header, std::make_shared<ExpressionActions>(query_info.prewhere_info->alias_actions, actions_settings));
            });
        }
        if (query_info.prewhere_info->row_level_filter)
        {
            pipeline->addSimpleTransform([&](const Block & header) {
                return std::make_shared<FilterTransform>(
                    header,
                    std::make_shared<ExpressionActions>(query_info.prewhere_info->row_level_filter, actions_settings),
                    query_info.prewhere_info->row_level_column_name,
                    false);
            });
        }
        pipeline->addSimpleTransform([&](const Block & header) {
            return std::make_shared<FilterTransform>(
                header,
                std::make_shared<ExpressionActions>(query_info.prewhere_info->prewhere_actions, actions_settings),
                query_info.prewhere_info->prewhere_column_name,
                query_info.prewhere_info->remove_prewhere_column);
        });
    }
}

Chunk LakeUnifiedSource::generate()
{
    try
    {
        while (true)
        {
            prepareReader();

            if (!reader)
            {
                // Reach EOF
                return {};
            }

            Chunk chunk;
            if (reader->pull(chunk))
            {
                return generatePostProcessor(chunk);
            }

            resetReader();
        }
    }
    catch (...)
    {
        resetReader();
        throw;
    }
}

void LakeUnifiedSource::resetReader()
{
    reader.reset();
    pipeline.reset();
    data_source.reset();
}

SourcePtr LakeUnifiedSource::getSource() const
{
    switch (lake_scan_info->storage_type)
    {
        case ILakeScanInfo::StorageType::Hive:
        case ILakeScanInfo::StorageType::Hudi:
            return lake_scan_info->getReader(block_info->physical_header, read_params);
        case ILakeScanInfo::StorageType::Paimon:
            return lake_scan_info->getReader(block_info->header, read_params);
    }

    throw Exception{ErrorCodes::UNKNOWN_STORAGE, "Unknown storage type: {}", static_cast<int>(lake_scan_info->storage_type)};
}

Chunk LakeUnifiedSource::generatePostProcessor(Chunk & chunk)
{
    switch (lake_scan_info->storage_type)
    {
        case ILakeScanInfo::StorageType::Hive:
        case ILakeScanInfo::StorageType::Hudi:
            return generatePostProcessorForHive(chunk);
        case ILakeScanInfo::StorageType::Paimon:
            return std::move(chunk);
    }

    throw Exception{ErrorCodes::UNKNOWN_STORAGE, "Unknown storage type: {}", static_cast<int>(lake_scan_info->storage_type)};
}

Chunk LakeUnifiedSource::generatePostProcessorForHive(Chunk & chunk)
{
    if (!lake_scan_info)
        return {};

    auto num_rows = chunk.getNumRows();
    Columns read_columns = chunk.detachColumns();

    const auto & output_header = getPort().getHeader();
    Columns result_columns;
    result_columns.reserve(output_header.columns());

    auto hive_virtuals = getHiveVirtuals();

    FileScanInfo * file_scan_info = dynamic_cast<FileScanInfo *>(lake_scan_info.get());

    for (const auto & col : output_header)
    {
        if (block_info->physical_header.has(col.name))
        {
            size_t pos = block_info->physical_header.getPositionByName(col.name);
            result_columns.push_back(std::move(read_columns[pos]));
        }
        else if (auto it = block_info->partition_name_to_index.find(col.name); it != block_info->partition_name_to_index.end())
        {
            /// Partition columns
            size_t idx = it->second;
            auto column = block_info->metadata->getPartitionKey().data_types[idx]->createColumnConst(
                num_rows, getHivePartition(lake_scan_info->getPartitionId().value())->value.at(idx));
            result_columns.push_back(std::move(column));
        }
        else if (col.name == "_path" && file_scan_info)
        {
            auto name_type = *hive_virtuals.tryGetByName("_path");
            result_columns.push_back(name_type.type->createColumnConst(num_rows, file_scan_info->getPath()));
        }
        else if (col.name == "_file" && file_scan_info)
        {
            auto name_type = *hive_virtuals.tryGetByName("_file");
            String path = file_scan_info->getPath();
            size_t last_slash_pos = path.find_last_of('/');
            result_columns.push_back(name_type.type->createColumnConst(num_rows, path.substr(last_slash_pos + 1)));
        }
        else if (col.name == "_size" && file_scan_info)
        {
            auto name_type = *hive_virtuals.tryGetByName("_size");
            result_columns.push_back(name_type.type->createColumnConst(num_rows, file_scan_info->getSize()));
        }
        else
            throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", col.name};
    }
    auto chunk_ret = Chunk(std::move(result_columns), num_rows);
    OwnerInfo owner_info{lake_scan_info->identifier(), 0};
    // todo caoliu time
    // OwnerInfo owner_info{file_path, static_cast<time_t>(current_file->getLastModifiedTimestamp())};
    chunk_ret.setOwnerInfo(std::move(owner_info));
    return chunk_ret;
}

HivePartitionPtr LakeUnifiedSource::getHivePartition(const String & partition_id)
{
    if (hive_partitions.find(partition_id) == hive_partitions.end())
    {
        auto partition = std::make_shared<HivePartition>();
        partition->load(partition_id, block_info->metadata->getPartitionKey());
        hive_partitions.emplace(partition_id, partition);
    }

    return hive_partitions[partition_id];
}

}
