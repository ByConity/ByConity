#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeBaseSelectProcessorLM.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{


/// Used to read data from single part with select query
/// Cares about PREWHERE, virtual columns, indexes etc.
/// To read data from multiple parts, Storage (MergeTree) creates multiple such objects.
class MergeTreeSelectProcessorLM : public MergeTreeBaseSelectProcessorLM
{
public:
    MergeTreeSelectProcessorLM(
        const MergeTreeMetaBase & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const MergeTreeData::DataPartPtr & owned_data_part_,
        ImmutableDeleteBitmapPtr delete_bitmap_,
        Names required_columns_,
        MarkRanges mark_ranges_,
        const SelectQueryInfo & query_info_,
        bool check_columns_,
        const MergeTreeStreamSettings & stream_settings_,
        const Names & virt_column_names_ = {},
        size_t part_index_in_query_ = 0,
        const MarkRangesFilterCallback & range_filter_callback_ = {});

    ~MergeTreeSelectProcessorLM() override = default;

    String getName() const override { return "MergeTreeLateMaterialize"; }

protected:

    bool getNewTaskImpl() override;

    /// Used by Task
    Names required_columns;
    /// Names from header. Used in order to order columns in read blocks.
    Names ordered_names;
    NameSet column_name_set;

    MergeTreeReadTaskColumns task_columns;

    /// Data part will not be removed if the pointer owns it
    MergeTreeData::DataPartPtr data_part;
    ImmutableDeleteBitmapPtr delete_bitmap;

    MarkRangesFilterCallback mark_ranges_filter_callback;

    /// Mark ranges we should read (in ascending order)
    MarkRanges all_mark_ranges;
    /// Approximate total rows for progress bar
    size_t total_rows;
    /// Value of _part_index virtual column (used only in SelectExecutor)
    size_t part_index_in_query = 0;

    bool check_columns;

    static inline Poco::Logger * log = &Poco::Logger::get("MergeTreeSelectProcessorLM");
};

}
