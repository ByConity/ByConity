#include <Storages/MergeTree/LateMaterialize/MergeTreeSelectProcessorLM.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeBaseSelectProcessorLM.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int INVALID_BITMAP_INDEX_READER;
}

MergeTreeSelectProcessorLM::MergeTreeSelectProcessorLM(
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeData::DataPartPtr & owned_data_part_,
    ImmutableDeleteBitmapPtr delete_bitmap_,
    Names required_columns_,
    MarkRanges mark_ranges_,
    const SelectQueryInfo & query_info_,
    bool check_columns_,
    const MergeTreeStreamSettings & stream_settings_,
    const Names & virt_column_names_,
    size_t part_index_in_query_,
    const MarkRangesFilterCallback & range_filter_callback_)
    :
    MergeTreeBaseSelectProcessorLM{
        storage_snapshot_->getSampleBlockForColumns(required_columns_),
        storage_, storage_snapshot_, query_info_, stream_settings_, virt_column_names_},
    required_columns{std::move(required_columns_)},
    data_part{owned_data_part_},
    delete_bitmap{std::move(delete_bitmap_)},
    mark_ranges_filter_callback(range_filter_callback_),
    all_mark_ranges(std::move(mark_ranges_)),
    part_index_in_query(part_index_in_query_),
    check_columns(check_columns_)
{
    /// Let's estimate total number of rows for progress bar.
    total_rows = data_part->index_granularity.getRowsCountInRanges(all_mark_ranges);

    LOG_TRACE(log, "Reading {} ranges from part {}, approx. {} rows starting from {}",
              all_mark_ranges.size(), data_part->name, total_rows,
              data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));

    addTotalRowsApprox(total_rows);
    ordered_names = header_without_virtual_columns.getNames();
    /// TODO @canh: can defer creating till running?
    task_columns = getReadTaskColumns(storage, storage_snapshot, data_part, required_columns, nullptr, getIndexContext(query_info_), getAtomicPredicates(query_info_), check_columns);
    const auto & columns_name = task_columns.columns.getNames();
    column_name_set = NameSet{columns_name.begin(), columns_name.end()};
}

bool MergeTreeSelectProcessorLM::getNewTaskImpl()
{
    try
    {
        if (all_mark_ranges.empty())
        {
            readers.clear();
            range_readers.clear();
            data_part.reset();
            return false;
        }

        if (mark_ranges_filter_callback)
        {
            all_mark_ranges = mark_ranges_filter_callback(data_part, all_mark_ranges);
        }

        auto size_predictor = (stream_settings.preferred_block_size_bytes == 0)
            ? nullptr
            : std::make_unique<MergeTreeBlockSizePredictor>(data_part, ordered_names, storage_snapshot->metadata->getSampleBlock());

        task = std::make_unique<MergeTreeReadTask>(
            data_part, delete_bitmap, all_mark_ranges, part_index_in_query, ordered_names, column_name_set, task_columns, false, task_columns.should_reorder, std::move(size_predictor), all_mark_ranges);

        all_mark_ranges.clear();

        return true;
    }
    catch (...)
    {
        /// Suspicion of the broken part. A part is added to the queue for verification.
        int current_exception_code = getCurrentExceptionCode();
        if (current_exception_code != ErrorCodes::MEMORY_LIMIT_EXCEEDED && current_exception_code != ErrorCodes::INVALID_BITMAP_INDEX_READER)
            storage.reportBrokenPart(data_part->name);
        throw;
    }
}

}
