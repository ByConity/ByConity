#include "Storages/Hive/StorageHiveSource.h"
#if USE_HIVE

#include "DataTypes/DataTypeString.h"
#include "Formats/FormatFactory.h"
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
    : files(std::move(files_)), progress(files.size())
{
}

void StorageHiveSource::Allocator::next(std::optional<FileSlice> & file_slice) const
{
    /// initialize file_slice
    if (!file_slice)
        file_slice.emplace(FileSlice{unallocated.fetch_add(1) % files.size(), 0});

    /// get unassigned slice in the current file
    if (nextSlice(*file_slice))
        return;

    /// get unassigned slice in other files
    for (size_t i = 0; i < files.size(); ++i)
    {
        file_slice->file = i;
        if (nextSlice(*file_slice))
            return;
    }
    file_slice.reset();
}

bool StorageHiveSource::Allocator::nextSlice(FileSlice & file_slice) const
{
    const auto & file = files[file_slice.file];

    /// if read by slice is not supported, we consider the file only has one slice
    size_t max_slice_num = allow_allocate_by_slice ? file->numSlices() : 1;
    size_t slice = progress[file_slice.file].fetch_add(1);
    if (slice < max_slice_num)
    {
        file_slice.slice = slice;
        return true;
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

    read_params = std::make_shared<IHiveFile::ReadParams>(IHiveFile::ReadParams{
            .max_block_size = DEFAULT_BLOCK_SIZE,
            .format_settings = std::move(format_settings),
            .context = getContext(),
        });
}

StorageHiveSource::~StorageHiveSource() = default;

void StorageHiveSource::prepareReader()
{
    int previous_file = current.has_value() ? current->file : -1;
    int previous_slice = current.has_value() ? current->slice : -1;

    allocator->next(current);
    /// no next file to read from
    if (!current)
        return;

    bool continue_reading = (previous_file == static_cast<int>(current->file));
    const auto & hive_file = allocator->files[current->file];

    LOG_TRACE(log, "Read from {} file, {} slice, continue_reading {}, previous file {}, previous slice {}",
        current->file, current->slice, continue_reading, previous_file, previous_slice);

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

    if (continue_reading)
    {
        read_params->slice = current->slice;
        pipeline = std::make_unique<QueryPipeline>();
        pipeline->init(Pipe(data_source));
        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
    }
    else
    {
        /// change the file to read from
        read_params->slice = current->slice;

        data_source = hive_file->getReader(block_info->to_read, read_params);
        pipeline = std::make_unique<QueryPipeline>();
        pipeline->init(Pipe(data_source));
        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
    }
}

Chunk StorageHiveSource::generate()
{
    if (!std::exchange(initialized, true))
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

        prepare();
    }

    return {};
}

void StorageHiveSource::buildResultChunk(Chunk & chunk) const
{
    if (!current)
        return;

    const auto & hive_file = allocator->files[current->file];
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
