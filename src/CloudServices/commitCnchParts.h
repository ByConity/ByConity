#pragma once

#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <WorkerTasks/ManipulationType.h>

namespace DB
{

class MergeTreeMetaBase;

struct PreparedCnchParts
{
    ManipulationType type{ManipulationType::Empty};

    /// optional
    String task_id;
    String consumer_group;
    String from_buffer_uuid;

    MergeTreeMutableDataPartsVector prepared_parts;
    MergeTreeMutableDataPartsVector prepared_staged_parts;
    DeleteBitmapMetaPtrVector delete_bitmaps;
};

/// Server side only
void commitPreparedCnchParts(const StoragePtr & storage, const Context & context, PreparedCnchParts & params, TxnTimestamp & commit_time);
}
