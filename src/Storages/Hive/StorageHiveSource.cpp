#include "Storages/Hive/StorageHiveSource.h"
#include "Core/NamesAndTypes.h"
#include "Storages/Hive/HiveVirtualColumns.h"
#if USE_HIVE

#include "Core/Names.h"
#include "DataTypes/DataTypeString.h"
#include "Formats/FormatFactory.h"
#include "Interpreters/Context.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include "Processors/Sources/SourceFromSingleChunk.h"
#include "Processors/QueryPipeline.h"
#include "Storages/Hive/HivePartition.h"
#include "Processors/Transforms/FilterTransform.h"
#include "Processors/Transforms/ExpressionTransform.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}

StorageHiveSource::BlockInfo::BlockInfo(const Block & header_, const StorageMetadataPtr & metadata_)
    : header(header_)
    , physical_header(header_)
    , metadata(metadata_)
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

    if (!physical_header)
    {
        all_partition_column = true;

        NamesAndTypesList physical = metadata->getColumns().getAllPhysical();
        physical.remove_if([&] (auto &col) {
            return partition_name_to_index.contains(col.name);
        });

        /// select smallest non partition column
        String selected = ExpressionActions::getSmallestColumn(physical);
        auto column = metadata->getColumns().getPhysical(selected);
        physical_header.insert(ColumnWithTypeAndName(column.type, column.name));
    }
}

Block StorageHiveSource::BlockInfo::getHeader() const
{
    return header;
}

StorageHiveSource::Allocator::Allocator(HiveFiles files_)
    : hive_files(std::move(files_))
{
}

HiveFilePtr StorageHiveSource::Allocator::next() const
{
    size_t idx = unallocated.fetch_add(1);
    if (idx >= hive_files.size())
        return nullptr;

    return hive_files[idx];
}

StorageHiveSource::StorageHiveSource(
    ContextPtr context_,
    size_t max_block_size_,
    BlockInfoPtr info_,
    AllocatorPtr allocator_,
    const std::shared_ptr<SelectQueryInfo> & query_info_,
    const SharedParsingThreadPoolPtr & shared_pool_)
    : SourceWithProgress(info_->getHeader())
    , WithContext(context_)
    , block_info(info_)
    , allocator(allocator_)
    , shared_pool(shared_pool_)
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

    read_params = std::make_shared<IHiveFile::ReadParams>(
        IHiveFile::ReadParams{
            .max_block_size = max_block_size_,
            .format_settings = std::move(format_settings),
            .context = context_,
            .read_settings = context_->getReadSettings(),
            .query_info = query_info_,
            .shared_pool = shared_pool
        });
}

StorageHiveSource::~StorageHiveSource() = default;

void StorageHiveSource::prepareReader()
{
    hive_file = allocator->next();
    if (!hive_file)
        return;

    /// all blocks are 'virtual' i.e. all columns are partition column
    if (block_info->all_partition_column)
    {
        std::optional<size_t> num_rows = hive_file->numRows();
        if (num_rows)
        {
            Columns columns;
            auto types = block_info->physical_header.getDataTypes();
            for (const auto & type : types)
                columns.push_back(type->createColumnConstWithDefaultValue(*num_rows));

            Chunk chunk(std::move(columns), *num_rows);
            pipeline = std::make_unique<QueryPipeline>();
            pipeline->init(Pipe(std::make_shared<SourceFromSingleChunk>(block_info->physical_header, std::move(chunk))));
            reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
            return;
        }
    }

    pipeline = std::make_unique<QueryPipeline>();
    data_source = hive_file->getReader(block_info->physical_header, read_params);
    pipeline->init(Pipe(data_source));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    auto input_format = std::dynamic_pointer_cast<IInputFormat>(data_source);
    if (read_params->query_info && read_params->query_info->prewhere_info
        && input_format && !input_format->supportsPrewhere())
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

Chunk StorageHiveSource::generate()
{
    try
    {
        if (!reader)
            prepareReader();

        while (reader)
        {
            Chunk chunk;
            if (reader->pull(chunk))
            {
                return buildResultChunk(chunk);
            }

            reader.reset();
            pipeline.reset();
            data_source.reset();
            prepareReader();
        }
        return {};
    }
    catch (...)
    {
        reader.reset();
        pipeline.reset();
        data_source.reset();
        throw;
    }
}

Chunk StorageHiveSource::buildResultChunk(Chunk & chunk) const
{
    if (!hive_file)
        return {};

    auto num_rows = chunk.getNumRows();
    Columns read_columns = chunk.detachColumns();

    const auto & output_header = getPort().getHeader();
    Columns result_columns;
    result_columns.reserve(output_header.columns());

    auto hive_virtuals = getHiveVirtuals();

    for (const auto & col : output_header) {
        if (block_info->physical_header.has(col.name))
        {
            size_t pos = block_info->physical_header.getPositionByName(col.name);
            result_columns.push_back(std::move(read_columns[pos]));
        }
        else if (auto it = block_info->partition_name_to_index.find(col.name); it != block_info->partition_name_to_index.end())
        {
            /// Partition columns
            size_t idx = it->second;
            auto column = block_info->metadata->getPartitionKey().data_types[idx]->createColumnConst(num_rows, hive_file->partition->value.at(idx));
            result_columns.push_back(std::move(column));
        }
        else if (col.name == "_path")
        {
            auto name_type = *hive_virtuals.tryGetByName("_path");
            result_columns.push_back(name_type.type->createColumnConst(num_rows, hive_file->file_path));
        }
        else if (col.name == "_file")
        {
            auto name_type = *hive_virtuals.tryGetByName("_path");
            size_t last_slash_pos = hive_file->file_path.find_last_of('/');
            result_columns.push_back(name_type.type->createColumnConst(num_rows, hive_file->file_path.substr(last_slash_pos + 1)));
        }
        else if (col.name == "_size")
        {
            auto name_type = *hive_virtuals.tryGetByName("_size");
            result_columns.push_back(name_type.type->createColumnConst(num_rows, hive_file->file_size));
        }
        else
            throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", col.name};
    }
    auto chunk_ret = Chunk(std::move(result_columns), num_rows);
    OwnerInfo owner_info{hive_file->file_path, 0};
    // todo caoliu time
    // OwnerInfo owner_info{file_path, static_cast<time_t>(current_file->getLastModifiedTimestamp())};
    chunk_ret.setOwnerInfo(std::move(owner_info));
    return chunk_ret;
}

}

#endif
