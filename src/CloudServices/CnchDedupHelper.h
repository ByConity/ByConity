#pragma once

#include <memory>
#include <Core/Names.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Transaction/LockRequest.h>

namespace DB
{
class MergeTreeMetaBase;
class StorageCnchMergeTree;
}


namespace DB::CnchDedupHelper
{

class DedupScope
{
public:
    static DedupScope Table()
    {
        static DedupScope table_scope{true};
        return table_scope;
    }

    static DedupScope Partitions(const NameOrderedSet & partitions) { return {false, partitions}; }

    bool isTable() const { return is_table; }
    bool isPartitions() const { return !is_table; }

    const NameOrderedSet & getPartitions() const { return partitions; }

private:
    DedupScope(bool is_table_, const NameOrderedSet & partitions_ = {}) : is_table(is_table_), partitions(partitions_) { }

    bool is_table{false};
    NameOrderedSet partitions;
};

std::vector<LockInfoPtr>
getLocksToAcquire(const DedupScope & scope, TxnTimestamp txn_id, const MergeTreeMetaBase & storage, UInt64 timeout_ms);

MergeTreeDataPartsCNCHVector getStagedPartsToDedup(const DedupScope & scope, StorageCnchMergeTree & cnch_table, TxnTimestamp ts);

MergeTreeDataPartsCNCHVector getVisiblePartsToDedup(const DedupScope & scope, StorageCnchMergeTree & cnch_table, TxnTimestamp ts);

struct FilterInfo
{
    IColumn::Filter filter;
    size_t num_filtered{0};
};

Block filterBlock(const Block & block, const FilterInfo & filter_info);

}
