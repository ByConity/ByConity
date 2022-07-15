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
        const Context & context_,
        ManipulationType type_,
        String task_id_ = {},
        String from_buffer_uuid_ = {},
        String consumer_group_ = {},
        const cppkafka::TopicPartitionList & tpl_ = {});

    ~CnchDataWriter() = default;

    DumpedData dumpAndCommitCnchParts(
        const IMutableMergeTreeDataPartsVector & temp_parts,
        const LocalDeleteBitmaps & temp_bitmaps = {},
        const IMutableMergeTreeDataPartsVector & temp_staged_parts = {});

    // server side only
    TxnTimestamp commitPreparedCnchParts(const DumpedData & data);

private:
    DumpedData dumpCnchParts(
        const IMutableMergeTreeDataPartsVector & temp_parts,
        const LocalDeleteBitmaps & temp_bitmaps = {},
        const IMutableMergeTreeDataPartsVector & temp_staged_parts = {});

    void commitDumpedParts(const DumpedData & dumped_data);


    MergeTreeMetaBase & storage;
    const Context & context;
    ManipulationType type;
    String task_id;
    String from_buffer_uuid;

    String consumer_group;
    cppkafka::TopicPartitionList tpl;
};

}
