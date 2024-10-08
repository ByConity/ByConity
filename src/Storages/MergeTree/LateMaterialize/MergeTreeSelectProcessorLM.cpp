#include <Storages/MergeTree/LateMaterialize/MergeTreeSelectProcessorLM.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeBaseSelectProcessorLM.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/FilterWithRowUtils.h>

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
    const RangesInDataPart & part_detail_,
    MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter_,
    Names required_columns_,
    const SelectQueryInfo & query_info_,
    bool check_columns_,
    const MergeTreeStreamSettings & stream_settings_,
    const Names & virt_column_names_,
    const MarkRangesFilterCallback & range_filter_callback_)
    :
    MergeTreeBaseSelectProcessorLM{
        storage_snapshot_->getSampleBlockForColumns(required_columns_),
        storage_, storage_snapshot_, query_info_, stream_settings_, virt_column_names_},
    required_columns{std::move(required_columns_)},
    part_detail{part_detail_},
    delete_bitmap_getter(std::move(delete_bitmap_getter_)),
    mark_ranges_filter_callback(range_filter_callback_),
    check_columns(check_columns_)
{
    /// Let's estimate total number of rows for progress bar.
    total_rows = part_detail.getRowsCount();

    LOG_TRACE(log, "Reading {} ranges from part {}, approx. {} rows starting from {}",
              part_detail.ranges.size(), part_detail.data_part->name, total_rows,
              part_detail.data_part->index_granularity.getMarkStartingRow(part_detail.ranges.front().begin));

    addTotalRowsApprox(total_rows);
    ordered_names = header_without_virtual_columns.getNames();
    /// TODO @canh: can defer creating till running?
    task_columns = getReadTaskColumns(storage, storage_snapshot, part_detail.data_part, required_columns, nullptr, getIndexContext(query_info_), getAtomicPredicates(query_info_), check_columns);
    const auto & columns_name = task_columns.columns.getNames();
    column_name_set = NameSet{columns_name.begin(), columns_name.end()};
}

bool MergeTreeSelectProcessorLM::getNewTaskImpl()
{
    try
    {
        if (part_detail.ranges.empty())
        {
            readers.clear();
            range_readers.clear();
            part_detail.data_part.reset();
            return false;
        }

        firstTaskInitialization();

        auto size_predictor = (stream_settings.preferred_block_size_bytes == 0)
            ? nullptr
            : std::make_unique<MergeTreeBlockSizePredictor>(part_detail.data_part, ordered_names, storage_snapshot->metadata->getSampleBlock());

        task = std::make_unique<MergeTreeReadTask>(
            part_detail.data_part, delete_bitmap, part_detail.ranges, part_detail.part_index_in_query, ordered_names, column_name_set, task_columns, false, task_columns.should_reorder, std::move(size_predictor), part_detail.ranges);

        part_detail.ranges.clear();

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
}

void MergeTreeSelectProcessorLM::firstTaskInitialization()
{
    std::unique_ptr<roaring::Roaring> row_filter = nullptr;
    if (mark_ranges_filter_callback)
    {
        if (stream_settings.reader_settings.read_settings.filtered_ratio_to_use_skip_read > 0)
        {
            row_filter = std::make_unique<roaring::Roaring>();
        }
        part_detail.ranges = mark_ranges_filter_callback(part_detail.data_part,
            part_detail.ranges, row_filter.get());
    }
    delete_bitmap = combineFilterBitmap(part_detail, delete_bitmap_getter);
    if (row_filter != nullptr)
    {
        flipFilterWithMarkRanges(part_detail.ranges,
            part_detail.data_part->index_granularity, *row_filter);
        if (delete_bitmap == nullptr)
        {
            delete_bitmap = std::shared_ptr<roaring::Roaring>(row_filter.release());
        }
        else
        {
            *delete_bitmap &= *row_filter;
        }
    }
}

}
