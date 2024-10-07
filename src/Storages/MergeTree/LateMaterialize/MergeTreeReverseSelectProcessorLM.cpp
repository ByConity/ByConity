#include <Storages/MergeTree/LateMaterialize/MergeTreeReverseSelectProcessorLM.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeBaseSelectProcessorLM.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int INVALID_BITMAP_INDEX_READER;
}

bool MergeTreeReverseSelectProcessorLM::getNewTaskImpl()
try
{
    if (is_first_task)
    {
        firstTaskInitialization();
    }
    is_first_task = false;

    if ((chunks.empty() && part_detail.ranges.empty()))
    {
        readers.clear();
        range_readers.clear();
        part_detail.data_part.reset();
        return false;
    }

    /// We have some blocks to return in buffer.
    /// Return true to continue reading, but actually don't create a task.
    if (part_detail.ranges.empty())
        return true;

    /// Read ranges from right to left.
    MarkRanges mark_ranges_for_task = { part_detail.ranges.back() };
    part_detail.ranges.pop_back();

    auto size_predictor = (stream_settings.preferred_block_size_bytes == 0)
        ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(part_detail.data_part, ordered_names, storage_snapshot->metadata->getSampleBlock());

    task = std::make_unique<MergeTreeReadTask>(
        part_detail.data_part, delete_bitmap, mark_ranges_for_task, part_detail.part_index_in_query, ordered_names, column_name_set,
        task_columns, false, task_columns.should_reorder, std::move(size_predictor), part_detail.ranges);

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    int current_exception_code = getCurrentExceptionCode();
    if (current_exception_code != ErrorCodes::MEMORY_LIMIT_EXCEEDED && current_exception_code != ErrorCodes::INVALID_BITMAP_INDEX_READER)
        storage.reportBrokenPart(part_detail.data_part->name);
    throw;
}

Chunk MergeTreeReverseSelectProcessorLM::readFromPart()
{
    Chunk res;

    if (!chunks.empty())
    {
        res = std::move(chunks.back());
        chunks.pop_back();
        return res;
    }

    while (task && !task->isFinished())
    {
        if (!chain_ready)
            initializeChain();
        if (!task->msr_range_reader)
            initializeTaskReader();
        Chunk chunk = readFromPartImpl();
        chunks.push_back(std::move(chunk));
    }

    if (chunks.empty())
        return {};

    res = std::move(chunks.back());
    chunks.pop_back();

    return res;
}

}
