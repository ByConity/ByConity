#include "Storages/Hive/StorageHiveSource.h"
#if USE_HIVE

#include "Core/Defines.h"
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

namespace DB
{
StorageHiveSource::BlockInfo::BlockInfo(const Block & header_, bool need_path_column_, bool need_file_column_, KeyDescription partition_)
    : header(header_)
    , to_read(header_)
    , need_path_column(need_path_column_)
    , need_file_column(need_file_column_)
    , partition_description(partition_)
{
    for (size_t i = 0; i < partition_description.column_names.size(); ++i)
    {
        const auto & name = partition_description.column_names[i];
        if (header.has(name))
        {
            to_read.erase(name);
            partition_column_idx.push_back(i);
        }
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

StorageHiveSource::StorageHiveSource(ContextPtr context_, BlockInfoPtr info_, AllocatorPtr allocator_)
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
    if (auto num_rows = hive_file->numRows(); !block_info->to_read.columns() && num_rows)
    {
        Columns columns;
        for (size_t idx : block_info->partition_column_idx)
        {
            auto column = block_info->partition_description.data_types[idx]->createColumnConst(*num_rows, hive_file->partition->value.at(idx));
            columns.emplace_back(column);
        }

        Chunk chunk(std::move(columns), *num_rows);
        pipeline = std::make_unique<QueryPipeline>();
        pipeline->init(Pipe(std::make_shared<SourceFromSingleChunk>(block_info->header, std::move(chunk))));
        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
        need_partition_columns = false;
        return;
    }
    else
        need_partition_columns = true;

    if (!continue_reading)
        read_params->read_buf.reset();

    read_params->slice = current_file_slice.slice;
    pipeline = std::make_unique<QueryPipeline>();
    data_source = hive_file->getReader(block_info->to_read, read_params);
    pipeline->init(Pipe(data_source));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
}

Chunk StorageHiveSource::generate()
{
    if (!reader)
        prepareReader();

    while (reader)
    {
        Chunk chunk;
        if (reader->pull(chunk))
        {
            buildResultChunk(chunk);
            return chunk;
        }

        reader.reset();
        pipeline.reset();
        prepareReader();
    }
    return {};
}

void StorageHiveSource::buildResultChunk(Chunk & chunk) const
{
    if (current_file_slice.empty())
        return;

    const auto & hive_file = allocator->files[current_file_slice.file];
    auto num_rows = chunk.getNumRows();

    if (need_partition_columns)
    {
        for (size_t idx : block_info->partition_column_idx)
        {
            auto column = block_info->partition_description.data_types[idx]->createColumnConst(num_rows, hive_file->partition->value.at(idx));
            chunk.addColumn(std::move(column));
        }
    }

    if (block_info->need_path_column)
    {
        chunk.addColumn(DataTypeString().createColumnConst(num_rows, hive_file->file_path)->convertToFullColumnIfConst());
    }

    if (block_info->need_file_column)
    {
        size_t last_slash_pos = hive_file->file_path.find_last_of('/');
        chunk.addColumn(
            DataTypeString().createColumnConst(num_rows, hive_file->file_path.substr(last_slash_pos + 1))->convertToFullColumnIfConst());
    }
}

}

#endif
