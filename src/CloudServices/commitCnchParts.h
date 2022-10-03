#pragma once

#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <WorkerTasks/ManipulationType.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>

#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition_list.h>

namespace DB
{

class MergeTreeMetaBase;

struct DumpedData
{
    MutableMergeTreeDataPartsCNCHVector parts;
    DeleteBitmapMetaPtrVector bitmaps;
    MutableMergeTreeDataPartsCNCHVector staged_parts;
};

// TODO: proper writer
class CnchDataWriter : private boost::noncopyable
{
public:
    CnchDataWriter(
        MergeTreeMetaBase & storage_,
        ContextPtr context_,
        ManipulationType type_,
        String task_id_ = {},
        String consumer_group_ = {},
        const cppkafka::TopicPartitionList & tpl_ = {});

    ~CnchDataWriter() = default;

    DumpedData dumpAndCommitCnchParts(
        const IMutableMergeTreeDataPartsVector & temp_parts,
        const LocalDeleteBitmaps & temp_bitmaps = {},
        const IMutableMergeTreeDataPartsVector & temp_staged_parts = {});

    // server side only
    TxnTimestamp commitPreparedCnchParts(const DumpedData & data);

    /// Convert staged parts to visible parts along with the given delete bitmaps.
    void publishStagedParts(
        const MergeTreeDataPartsCNCHVector & staged_parts,
        const LocalDeleteBitmaps & bitmaps_to_dump);

    DumpedData dumpCnchParts(
        const IMutableMergeTreeDataPartsVector & temp_parts,
        const LocalDeleteBitmaps & temp_bitmaps = {},
        const IMutableMergeTreeDataPartsVector & temp_staged_parts = {});

    void commitDumpedParts(const DumpedData & dumped_data);

private:

    MergeTreeMetaBase & storage;
    ContextPtr context;
    ManipulationType type;
    String task_id;

    String consumer_group;
    cppkafka::TopicPartitionList tpl;
};

}
