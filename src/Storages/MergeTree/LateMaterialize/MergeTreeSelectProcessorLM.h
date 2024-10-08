#pragma once
#include <Common/Logger.h>
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
        const RangesInDataPart & part_detail_,
        MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter_,
        Names required_columns_,
        const SelectQueryInfo & query_info_,
        bool check_columns_,
        const MergeTreeStreamSettings & stream_settings_,
        const Names & virt_column_names_ = {},
        const MarkRangesFilterCallback & range_filter_callback_ = {});

    ~MergeTreeSelectProcessorLM() override = default;

    String getName() const override { return "MergeTreeLateMaterialize"; }

protected:

    bool getNewTaskImpl() override;
    void firstTaskInitialization();

    /// Used by Task
    Names required_columns;
    /// Names from header. Used in order to order columns in read blocks.
    Names ordered_names;
    NameSet column_name_set;

    MergeTreeReadTaskColumns task_columns;

    /// Data part will not be removed if the pointer owns it
    RangesInDataPart part_detail;
    MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter;
    /// Lazy init, need to use getDeleteBitmap() interface rather than use delete_bitmap directly
    DeleteBitmapPtr delete_bitmap;

    MarkRangesFilterCallback mark_ranges_filter_callback;

    /// Approximate total rows for progress bar
    size_t total_rows;

    bool check_columns;

    static inline LoggerPtr log = getLogger("MergeTreeSelectProcessorLM");
};

}
