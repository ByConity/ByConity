#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <Storages/MergeTree/MergeTreeBitMapIndexReader.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

MergeTreeThreadSelectBlockInputProcessor::MergeTreeThreadSelectBlockInputProcessor(
    const size_t thread_,
    const MergeTreeReadPoolPtr & pool_,
    const size_t min_marks_to_read_,
    const UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    const MergeTreeMetaBase & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    const BitMapIndexInfoPtr & bitmap_index_info_,
    ExpressionActionsSettings actions_settings,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_)
    :
    MergeTreeBaseSelectProcessor{
        pool_->getHeader(), storage_, metadata_snapshot_, prewhere_info_, std::move(actions_settings), max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_},
    thread{thread_},
    pool{pool_},
    bitmap_index_info{bitmap_index_info_}
{
    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    /// If granularity is adaptive it doesn't make sense
    /// Maybe it will make sense to add settings `max_block_size_bytes`
    if (max_block_size_rows && !storage.canUseAdaptiveGranularity())
    {
        size_t fixed_index_granularity = storage.getSettings()->index_granularity;
        min_marks_to_read = (min_marks_to_read_ * fixed_index_granularity + max_block_size_rows - 1)
            / max_block_size_rows * max_block_size_rows / fixed_index_granularity;
    }
    else
        min_marks_to_read = min_marks_to_read_;

    ordered_names = getPort().getHeader().getNames();
}

/// Requests read task from MergeTreeReadPool and signals whether it got one
bool MergeTreeThreadSelectBlockInputProcessor::getNewTask()
{
    task = pool->getTask(min_marks_to_read, thread, ordered_names);

    if (!task)
    {
        /** Close the files (before destroying the object).
          * When many sources are created, but simultaneously reading only a few of them,
          * buffers don't waste memory.
          */
        reader.reset();
        pre_reader.reset();
        bitmap_index_reader.reset();
        return false;
    }

    const std::string part_name = task->data_part->isProjectionPart() ? task->data_part->getParentPart()->name : task->data_part->name;

    /// Allows pool to reduce number of threads in case of too slow reads.
    auto profile_callback = [this](ReadBufferFromFileBase::ProfileInfo info_) { pool->profileFeedback(info_); };

    if (metadata_snapshot->hasUniqueKey())
    {
        task->delete_bitmap = task->data_part->getDeleteBitmap();
        if (!task->delete_bitmap)
            throw Exception("Expected delete bitmap exists for a unique table part: " + task->data_part->name, ErrorCodes::LOGICAL_ERROR);
    }

    if (!reader)
    {
        auto rest_mark_ranges = pool->getRestMarks(*task->data_part, task->mark_ranges[0]);

        if (use_uncompressed_cache)
            owned_uncompressed_cache = storage.getContext()->getUncompressedCache();
        owned_mark_cache = storage.getContext()->getMarkCache();

        reader = task->data_part->getReader(task->columns, metadata_snapshot, rest_mark_ranges,
            owned_uncompressed_cache.get(), owned_mark_cache.get(), reader_settings, prewhere_info ? nullptr : bitmap_index_reader.get(),
            IMergeTreeReader::ValueSizeMap{},  profile_callback);

        if (prewhere_info)
        {
            bitmap_index_reader = BitMapIndexHelper::getBitMapIndexReader(task->data_part->getFullPath(), bitmap_index_info, task->data_part->index_granularity, task->mark_ranges);
            // current bitmap index only support in wide format
            if (prewhere_info->has_bitmap_index && task->data_part->getType() == MergeTreeDataPartType::Value::WIDE)
            {
                prepareForBitMapIndexFunctions(bitmap_index_info, task->pre_columns, task->columns);
            }
            pre_reader = task->data_part->getReader(
                task->pre_columns,
                metadata_snapshot,
                rest_mark_ranges,
                owned_uncompressed_cache.get(),
                owned_mark_cache.get(),
                reader_settings,
                bitmap_index_reader.get(),
                IMergeTreeReader::ValueSizeMap{},
                profile_callback);
        }
    }
    else
    {
        /// in other case we can reuse readers, anyway they will be "seeked" to required mark
        if (part_name != last_readed_part_name)
        {
            auto rest_mark_ranges = pool->getRestMarks(*task->data_part, task->mark_ranges[0]);
            /// retain avg_value_size_hints
            reader = task->data_part->getReader(
                task->columns,
                metadata_snapshot,
                rest_mark_ranges,
                owned_uncompressed_cache.get(),
                owned_mark_cache.get(),
                reader_settings,
                nullptr,
                reader->getAvgValueSizeHints(),
                profile_callback);

            if (prewhere_info)
            {
                bitmap_index_reader
                    = BitMapIndexHelper::getBitMapIndexReader(task->data_part->getFullPath(), bitmap_index_info, task->data_part->index_granularity, task->mark_ranges);
                pre_reader = task->data_part->getReader(
                    task->pre_columns,
                    metadata_snapshot,
                    rest_mark_ranges,
                    owned_uncompressed_cache.get(),
                    owned_mark_cache.get(),
                    reader_settings,
                    bitmap_index_reader.get(),
                    reader->getAvgValueSizeHints(),
                    profile_callback);
            }
        }
    }

    last_readed_part_name = part_name;

    return true;
}


MergeTreeThreadSelectBlockInputProcessor::~MergeTreeThreadSelectBlockInputProcessor() = default;

}
