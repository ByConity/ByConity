#pragma once

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/Checkpoint.h>
#include <CloudServices/ICnchBGThread.h>
#include <Core/Names.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/TxnTimestamp.h>

#include <pcg_random.hpp>

namespace DB
{

/// A thread clean up the stale stuff (like parts, deleted bitmaps, labels) for table
/// Also, mark expired parts accord to table level TTL
/// Just name PartGC thread for convenience because I couldn't get a better name
class CnchPartGCThread : public ICnchBGThread
{
public:
    CnchPartGCThread(ContextPtr context_, const StorageID & id);

private:
    CnchBGThreadPtr getMergeThread();

    void runImpl() override;

    void clearOldParts(const StoragePtr & istorage, StorageCnchMergeTree & storage);
    void clearOldInsertionLabels(const StoragePtr & istorage, StorageCnchMergeTree & storage);

    TxnTimestamp calculateGCTimestamp(UInt64 delay_second, bool in_wakeup);

    void tryMarkExpiredPartitions(StorageCnchMergeTree & storage, const ServerDataPartsVector & visible_parts);

    void pushToRemovingQueue(
        StorageCnchMergeTree & storage, const ServerDataPartsVector & parts, const String & reason, bool is_staged_part = false);
    // void removeDeleteBitmaps(StorageCnchMergeTree & storage, const DeleteBitmapMetaPtrVector & bitmaps, const String & reason);

    void collectStaleParts(
        ServerDataPartPtr parent_part,
        TxnTimestamp begin,
        TxnTimestamp end,
        bool has_visible_ancestor,
        ServerDataPartsVector & stale_parts) const;

    // void collectStaleBitmaps(
    //     const DeleteBitmapMetaPtr & parent_bitmap,
    //     const TxnTimestamp & begin,
    //     const TxnTimestamp & end,
    //     bool has_visible_ancestor,
    //     DeleteBitmapMetaPtrVector & stale_bitmaps);

    std::vector<TxnTimestamp> getCheckpoints(StorageCnchMergeTree & storage, TxnTimestamp max_timestamp);

    void collectBetweenCheckpoints(
        StorageCnchMergeTree & storage,
        const ServerDataPartsVector & visible_parts,
        const DeleteBitmapMetaPtrVector & visible_bitmaps,
        TxnTimestamp begin,
        TxnTimestamp end);

    // void updatePartCache(const String & partition_id, Int64 part_num) override
    // {
    //     if (auto merge = getMergeThread())
    //         merge->updatePartCache(partition_id, -1 * part_num);
    // }

private:
    BackgroundSchedulePool::TaskHolder checkpoint_task;

    pcg64 rng;
    TxnTimestamp last_gc_timestamp{0};

    std::queue<IMergeTreeDataPartPtr> removing_queue;

    std::weak_ptr<ICnchBGThread> merge_thread;

    time_t gc_labels_last_time{0};
};


}
