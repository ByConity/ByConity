#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeSequentialSource::MergeTreeSequentialSource(
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    MergeTreeData::DataPartPtr data_part_,
    Names columns_to_read_,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet)
    : MergeTreeSequentialSource(storage_, metadata_snapshot_,
    data_part_, data_part_->getDeleteBitmap(), columns_to_read_, read_with_direct_io_,
    take_column_types_from_storage, quiet, false) {}

MergeTreeSequentialSource::MergeTreeSequentialSource(
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    MergeTreeData::DataPartPtr data_part_,
    DeleteBitmapPtr delete_bitmap_,
    Names columns_to_read_,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet, bool include_rowid_column_)
    : SourceWithProgress(metadata_snapshot_->getSampleBlockForColumns(
            columns_to_read_, storage_.getVirtuals(), storage_.getStorageID(), include_rowid_column_))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , data_part(std::move(data_part_))
    , delete_bitmap(std::move(delete_bitmap_))
    , columns_to_read(std::move(columns_to_read_))
    , read_with_direct_io(read_with_direct_io_)
    , mark_cache(storage.getContext()->getMarkCache())
    , include_rowid_column(include_rowid_column_)
{
    size_t num_deletes = delete_bitmap ? delete_bitmap->cardinality() : 0;
    if (!quiet)
    {
        /// Print column name but don't pollute logs in case of many columns.
        if (columns_to_read.size() == 1)
            LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part, column {}",
                data_part->getMarksCount(), data_part->name, data_part->rows_count, columns_to_read.front());
        else
            LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part",
                data_part->getMarksCount(), data_part->name, data_part->rows_count);
    }

    addTotalRowsApprox(data_part->rows_count - num_deletes);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(storage, metadata_snapshot, data_part, columns_to_read);
    NamesAndTypesList columns_for_reader;
    if (take_column_types_from_storage)
    {
        columns_for_reader = metadata_snapshot->getColumns().getByNames(ColumnsDescription::AllPhysical, columns_to_read, false);
    }
    else
    {
        /// take columns from data_part
        columns_for_reader = data_part->getColumns().addTypes(columns_to_read);
    }

    MergeTreeReaderSettings reader_settings =
    {
        /// bytes to use AIO (this is hack)
        .min_bytes_to_use_direct_io = read_with_direct_io ? 1UL : std::numeric_limits<size_t>::max(),
        .max_read_buffer_size = DBMS_DEFAULT_BUFFER_SIZE,
        .save_marks_in_cache = false
    };

    reader = data_part->getReader(columns_for_reader, metadata_snapshot,
        MarkRanges{MarkRange(0, data_part->getMarksCount())},
        /* uncompressed_cache = */ nullptr, mark_cache.get(), reader_settings);

    index_granularity = storage_.getSettings()->index_granularity;
}

Chunk MergeTreeSequentialSource::generate()
try
{
    if (delete_bitmap)
    {
        /// skip deleted mark
        size_t marks_count = data_part->index_granularity.getMarksCount();
        while (current_mark < marks_count && delete_bitmap->containsRange(currentMarkStart(), currentMarkEnd()))
        {
            current_row += data_part->index_granularity.getMarkRows(current_mark);
            current_mark++;
            continue_reading = false;
        }
        if (current_mark >= marks_count)
        {
            finish();
            return Chunk();
        }
    }

    const auto & header = getPort().getHeader();

    if (!isCancelled() && current_row < data_part->rows_count)
    {
        size_t rows_to_read = data_part->index_granularity.getMarkRows(current_mark);
        continue_reading = (current_mark != 0);

        const auto & sample = reader->getColumns();
        Columns columns(sample.size());
        size_t rows_read = reader->readRows(current_mark, continue_reading, rows_to_read, columns);

        if (rows_read)
        {

            size_t num_deleted = 0;
            if (delete_bitmap)
            {
                /// construct delete filter for current granule
                auto delete_column = ColumnUInt8::create(rows_read, 1);
                UInt8 * filter_data = delete_column->getData().data();
                size_t start_row = currentMarkStart();
                size_t end_row = currentMarkEnd();

                auto iter = delete_bitmap->begin();
                iter.equalorlarger(start_row);
                for (auto end = delete_bitmap->end(); iter != end && *iter < end_row; iter++)
                {
                    filter_data[*iter - start_row] = 0;
                    num_deleted++;
                }
                delete_column->getData().resize(rows_read);
                for (auto & column : columns)
                {
                    column = column->filter(delete_column->getData(), rows_read - num_deleted);
                }
            }

            bool should_evaluate_missing_defaults = false;
            reader->fillMissingColumns(columns, should_evaluate_missing_defaults, rows_read);

            if (should_evaluate_missing_defaults)
            {
                reader->evaluateMissingDefaults({}, columns);
            }

            reader->performRequiredConversions(columns);

            if (include_rowid_column)
            {
                auto column = ColumnUInt32::create(rows_read);
                for (size_t i = 0; i < rows_read; ++i)
                    column->getData()[i] = static_cast<UInt32>(current_row + i);
                columns.emplace_back(std::move(column));
            }

            size_t num_columns = sample.size() + include_rowid_column;
            if (include_rowid_column && sample.size() + 1 != num_columns)
            {
                throw Exception("Shoule have rowid column exist", ErrorCodes::MEMORY_LIMIT_EXCEEDED);
            }

            Columns res_columns;
            res_columns.reserve(num_columns);

            auto it = sample.begin();
            for (size_t i = 0; i < num_columns; ++i)
            {
                if (it != sample.end() && header.has(it->name)) {
                    res_columns.emplace_back(std::move(columns[i]));
                    ++it;
                }
                else if (it == sample.end()) {
                    if (!include_rowid_column)
                        throw Exception("The last column should be row_id column", ErrorCodes::MEMORY_LIMIT_EXCEEDED);
                    res_columns.emplace_back(std::move(columns[i]));
                }
            }

            current_row += rows_read;
            current_mark += (rows_to_read == rows_read);

            LOG_DEBUG(log, "Try to read rows {}, actual read rows {}, delete {} row from part {}", rows_to_read, rows_read, num_deleted, data_part->name);
            return Chunk(std::move(res_columns), rows_read - num_deleted);
        }
    }
    else
    {
        finish();
    }

    return {};
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part->name);
    throw;
}

void MergeTreeSequentialSource::finish()
{
    /** Close the files (before destroying the object).
     * When many sources are created, but simultaneously reading only a few of them,
     * buffers don't waste memory.
     */
    reader.reset();
    data_part.reset();
}

MergeTreeSequentialSource::~MergeTreeSequentialSource() = default;

}
