#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeThreadSelectProcessorLM.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

MergeTreeThreadSelectProcessorLM::MergeTreeThreadSelectProcessorLM(
    const size_t thread_,
    const MergeTreeReadPoolPtr & pool_,
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const SelectQueryInfo & query_info_,
    const MergeTreeStreamSettings & stream_settings_,
    const Names & virt_column_names_)
    :
    MergeTreeBaseSelectProcessorLM{
        pool_->getHeader(), storage_, storage_snapshot_, query_info_,stream_settings_ , virt_column_names_},
    thread{thread_},
    pool{pool_}
{
    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    /// If granularity is adaptive it doesn't make sense
    /// Maybe it will make sense to add settings `max_block_size_bytes`
    if (auto max_block_size_rows = stream_settings.max_block_size; max_block_size_rows  && !storage.canUseAdaptiveGranularity())
    {
        size_t fixed_index_granularity = storage.getSettings()->index_granularity;
        min_marks_to_read = (stream_settings.min_marks_for_concurrent_read * fixed_index_granularity + max_block_size_rows - 1)
            / max_block_size_rows * max_block_size_rows / fixed_index_granularity;
    }
    else
        min_marks_to_read = stream_settings.min_marks_for_concurrent_read;

    ordered_names = getPort().getHeader().getNames();

}

/// Requests read task from MergeTreeReadPool and signals whether it got one
bool MergeTreeThreadSelectProcessorLM::getNewTaskImpl()
{
    task = pool->getTask(min_marks_to_read, thread, ordered_names);

    if (!task)
    {
#ifndef NDEBUG
        updateGranuleCounter();
#endif
        /** Close the files (before destroying the object).
          * When many sources are created, but simultaneously reading only a few of them,
          * buffers don't waste memory.
          */
        readers.clear();
        range_readers.clear();
        return false;
    }
    /// Allows pool to reduce number of threads in case of too slow reads.
    profile_callback = [this](ReadBufferFromFileBase::ProfileInfo info_) { pool->profileFeedback(info_); };
    return true;
}

void MergeTreeThreadSelectProcessorLM::updateGranuleCounter()
{
#ifndef NDEBUG
    for (auto & r : range_readers)
    {
        pool->updateGranuleStats(r.per_column_read_granules);
    }
#endif
}

}
