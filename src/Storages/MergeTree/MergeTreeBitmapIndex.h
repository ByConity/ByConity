#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/QueueForAsyncTask.h>
#include <common/logger_useful.h>

namespace DB
{

class BitmapTracker
{
    std::mutex tracker_mutex;
    ContextPtr context;
    size_t current_rows = 0;
    size_t current_task = 0;

public:
    BitmapTracker(ContextPtr context_)
        : context(context_) {}

    bool check(const MergeTreeData::DataPartPtr & part, bool check_merge = false);
    bool start(const MergeTreeData::DataPartPtr & part);
    void end(const MergeTreeData::DataPartPtr & part);
};

class BitmapCounter
{
    BitmapTracker * tracker = nullptr;
    const MergeTreeData::DataPartPtr part = nullptr;
    std::atomic<bool> should_build = false;
public:
    BitmapCounter(BitmapTracker * tracker_, const MergeTreeData::DataPartPtr & part_)
        : tracker(tracker_), part(part_)
    {
        if (tracker)
            should_build = tracker->start(part);
    }

    ~BitmapCounter()
    {
        if (tracker && should_build)
            tracker->end(part);
    }

    bool isBuilt() { return should_build; }
};

struct MergeTreeBitmapIndexStatus
{
    String database;
    String table;
    Names parts;
};

class MergeTreeBitmapIndex
{

public:

    MergeTreeBitmapIndex(MergeTreeData & data);

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
        oss << "Status of Bitmap:\n";
        oss << parts_for_bitmap.toString() << "\n";
        LOG_DEBUG(log, oss.str());
    }

    MergeTreeBitmapIndexStatus getMergeTreeBitmapIndexStatus();

    const ActionBlocker & getActionBlocker() const { return stop_build_bitmap; }

    void buildPartIndex(const MergeTreeData::DataPartPtr & part);

    // TODO dongyifeng fix it later
//    MergeTreeData::AlterDataPartTransactionPtr buildPartIndex(const MergeTreeData::DataPartPtr & part, const NamesAndTypesList & columns);

    static bool isMarkBitmapIndexColumn(DataTypePtr column_type);
    static bool isMarkBitmapIndexColumn(const IDataType & type);
    bool isMarkBitmapIndexColumn(const String & name);
    static bool isBitmapIndexColumn(DataTypePtr column_type);
    static bool isBitmapIndexColumn(const IDataType & type);
    bool isBitmapIndexColumn(const String & name);

    static bool onlyBuildBitmap(const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns);

    static void dropPartIndex(const MergeTreeData::DataPartPtr & part);

    // TODO dongyifeng fix it later
//    static MergeTreeData::AlterDataPartTransactionPtr dropPartIndex(const MergeTreeData::DataPartPtr & part, const NamesAndTypesList & columns);

private:

    MergeTreeData::DataPartPtr getPartForBitMapIndex() { return parts_for_bitmap.pop(); }
    bool emptyQueueForBuildingBitMap() { return parts_for_bitmap.empty(); }

    MergeTreeData & data;
    QueueForIndex parts_for_bitmap;
    ActionBlocker stop_build_bitmap;
    std::unique_ptr<BitmapTracker> bitmap_tracker = nullptr;

    Poco::Logger * log;
};

}
