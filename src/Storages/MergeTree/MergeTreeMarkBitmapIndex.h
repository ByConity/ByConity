#pragma once

#include <sstream>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/QueueForAsyncTask.h>
#include <Storages/MergeTree/MergeTreeBitmapIndex.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>

namespace DB
{


class MergeTreeMarkBitmapIndex
{
public:
    MergeTreeMarkBitmapIndex(MergeTreeData & data);

    bool addPartForBitMapIndex(const MergeTreeData::DataPartPtr & part)
    {
        return parts_for_bitmap.push(part);
    }

    void getPartsForBuildingBitmap();

    bool checkForBuildingBitMap();

    ActionLock stopBuildBitMap()
    {
        return stop_build_bitmap.cancel();
    }

    void stopBuildBitMapForever()
    {
        stop_build_bitmap.cancelForever();
    }

    void dumpMergeTreeBitmapIndexStatus()
    {
        std::ostringstream oss;
        oss << "Status of Mark Bitmap:\n";
        oss << parts_for_bitmap.toString() << "\n";
        LOG_DEBUG(log, oss.str());
    }

    MergeTreeBitmapIndexStatus getMergeTreeBitmapIndexStatus();

    const ActionBlocker & getActionBlocker() const { return stop_build_bitmap; }

    void buildPartIndex(const MergeTreeData::DataPartPtr & part);

    // TODO dongyifeng fix it later
//    MergeTreeData::AlterDataPartTransactionPtr buildPartIndex(const MergeTreeData::DataPartPtr & part, const NamesAndTypesList & columns);

    static bool isMarkBitmapIndexColumn(const NameAndTypePair & column);
    static bool isMarkBitmapIndexColumn(const IDataType & type);
    bool isMarkBitmapIndexColumn(const String & name);

    static bool onlyBuildBitmap(const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns);

    static void dropPartIndex(const MergeTreeData::DataPartPtr & part);

    // TODO dongyifeng fix it later
//static MergeTreeData::AlterDataPartTransactionPtr dropPartIndex(const MergeTreeData::DataPartPtr & part, const NamesAndTypesList & columns);

private:
    MergeTreeData::DataPartPtr getPartForBitMapIndex() { return parts_for_bitmap.pop(); }
    bool emptyQueueForBuildingBitMap() { return parts_for_bitmap.empty(); }
    static inline String getIrkExtension() { return MARK_BITMAP_IRK_EXTENSION; }
    static inline String getIdxExtension() { return MARK_BITMAP_IDX_EXTENSION; }

    MergeTreeData & data;
    QueueForIndex parts_for_bitmap;
    ActionBlocker stop_build_bitmap;
    std::unique_ptr<BitmapTracker> bitmap_tracker = nullptr;

    Poco::Logger * log;
};
}
