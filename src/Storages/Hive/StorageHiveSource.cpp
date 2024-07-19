#include "Storages/Hive/StorageHiveSource.h"
#if USE_HIVE

#include "Core/Defines.h"
#include "Core/Names.h"
#include "DataTypes/DataTypeString.h"
#include "Formats/FormatFactory.h"
#include "Interpreters/Context.h"
#include "IO/ReadSettings.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include "Processors/Sources/SourceFromSingleChunk.h"
#include "Processors/Pipe.h"
#include "Processors/QueryPipeline.h"
#include "Storages/Hive/HivePartition.h"
#include "Processors/Sources/NullSource.h"
#include "Processors/Transforms/FilterTransform.h"
#include "Processors/Transforms/ExpressionTransform.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}

StorageHiveSource::BlockInfo::BlockInfo(const Block & header_, bool need_path_column_, bool need_file_column_, const StorageMetadataPtr & metadata)
    : header(header_)
    , to_read(header_)
    , need_path_column(need_path_column_)
    , need_file_column(need_file_column_)
    , partition_description(metadata->getPartitionKey())
{
    for (size_t i = 0; i < partition_description.column_names.size(); ++i)
    {
        const auto & name = partition_description.column_names[i];
        if (header.has(name))
            to_read.erase(name);

        partition_name_to_index[name] = i;
    }

    if (!to_read)
    {
        all_partition_column = true;
        Names partition_columns = partition_description.expression->getRequiredColumns();
        NameSet partition_column_set(partition_columns.begin(), partition_columns.end());

        NamesAndTypesList physical = metadata->getColumns().getAllPhysical();
        physical.remove_if([&] (auto &col) {
            return partition_column_set.find(col.name) != partition_column_set.end();
        });

        /// select smallest non partition column
        String selected = ExpressionActions::getSmallestColumn(physical);
        auto column = metadata->getColumns().getPhysical(selected);
        to_read.insert(ColumnWithTypeAndName(column.type, column.name));
    }
}

StorageHiveSource::Allocator::Allocator(HiveFiles files_)
    : files(std::move(files_)), slice_progress(files.size())
{
}

void StorageHiveSource::Allocator::next(FileSlice & file_slice) const
{
    if (files.empty())
        return;

    /// the first file to start searching an available slice to read
    int start = file_slice.empty() ? unallocated.fetch_add(1) % files.size() : file_slice.file;

    for (size_t i = 0; i < files.size(); ++i)
    {
        file_slice.file = (start + i) % files.size();
        if (nextSlice(file_slice))
            return;
    }

    // can not find a file to read from
    file_slice.reset();
}

bool StorageHiveSource::Allocator::nextSlice(FileSlice & file_slice) const
{
    const auto & file = files[file_slice.file];

    /// if read by slice is not supported, we consider the file only has one slice
    size_t max_slice_num = allow_allocate_by_slice ? file->numSlices() : 1;
    for (size_t slice = slice_progress[file_slice.file].fetch_add(1); slice < max_slice_num;
         slice = slice_progress[file_slice.file].fetch_add(1))
    {
        if (!file->canSkipSplit(slice))
        {
            file_slice.slice = slice;
            return true;
        }
    }

    return false;
}

Block StorageHiveSource::BlockInfo::getHeader() const
{
    Block sample_block = header;
    if (need_path_column)
        sample_block.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_path"});
    if (need_file_column)
        sample_block.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_file"});

    return sample_block;
}

StorageHiveSource::StorageHiveSource(ContextPtr context_, BlockInfoPtr info_, AllocatorPtr allocator_, const std::shared_ptr<SelectQueryInfo> &query_info_)
    : SourceWithProgress(info_->getHeader()), WithContext(context_), block_info(info_), allocator(allocator_)
{
    FormatSettings format_settings = getFormatSettings(context_);
    format_settings.orc.allow_missing_columns = true;
    format_settings.parquet.allow_missing_columns = true;

    const auto & settings = context_->getSettingsRef();
    read_params = std::make_shared<IHiveFile::ReadParams>(
        IHiveFile::ReadParams{
            .max_block_size = settings.max_block_size,
            .format_settings = std::move(format_settings),
            .context = context_,
            .read_settings = context_->getReadSettings(),
            .query_info = query_info_
        });
}

StorageHiveSource::~StorageHiveSource() = default;

void StorageHiveSource::prepareReader()
{
    int previous_file = current_file_slice.file;
    allocator->next(current_file_slice);
    /// no next file to read from

    if (current_file_slice.empty())
        return;

    bool continue_reading = (previous_file == static_cast<int>(current_file_slice.file));
    const auto & hive_file = allocator->files[current_file_slice.file];

    LOG_TRACE(log, "Read from file {}, slice {}, continue_reading {}",
        current_file_slice.file, current_file_slice.slice, continue_reading);

    /// all blocks are 'virtual' i.e. all columns are partition column
    if (auto num_rows = hive_file->numRows(); block_info->all_partition_column && num_rows)
    {
        Columns columns;
        auto types = block_info->to_read.getDataTypes();
        for (const auto & type : types)
            columns.push_back(type->createColumnConstWithDefaultValue(*num_rows));

        Chunk chunk(std::move(columns), *num_rows);
        pipeline = std::make_unique<QueryPipeline>();
        pipeline->init(Pipe(std::make_shared<SourceFromSingleChunk>(block_info->header, std::move(chunk))));
        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
        return;
    }

    if (!continue_reading)
        read_params->read_buf.reset();

    read_params->slice = current_file_slice.slice;
    pipeline = std::make_unique<QueryPipeline>();
    data_source = hive_file->getReader(block_info->to_read, read_params);
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
    if (current_file_slice.empty())
        return {};

    auto num_rows = chunk.getNumRows();
    const auto & read_block = block_info->to_read;
    Columns read_columns = chunk.detachColumns();

    const auto & hive_file = allocator->files[current_file_slice.file];
    const auto & output_header = getPort().getHeader();
    Columns result_columns;
    result_columns.reserve(output_header.columns());

    for (const auto & col : output_header) {
        if (read_block.has(col.name)) {
            size_t pos = read_block.getPositionByName(col.name);
            result_columns.push_back(std::move(read_columns[pos]));
        }
        else if (col.name == "_path") {
            result_columns.push_back(DataTypeString().createColumnConst(num_rows, hive_file->file_path)->convertToFullColumnIfConst());
        }
        else if (col.name == "_file") {
            size_t last_slash_pos = hive_file->file_path.find_last_of('/');
            result_columns.push_back(DataTypeString().createColumnConst(num_rows, hive_file->file_path.substr(last_slash_pos + 1))->convertToFullColumnIfConst());
        }
        /// if it is a partition columns
        else if (auto it = block_info->partition_name_to_index.find(col.name); it != block_info->partition_name_to_index.end()) {
            size_t idx = it->second;
            auto column = block_info->partition_description.data_types[idx]->createColumnConst(num_rows, hive_file->partition->value.at(idx));
            result_columns.push_back(std::move(column));
        }
        else
            throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", col.name};
    }
    return Chunk(std::move(result_columns), num_rows);
}

}

#endif
