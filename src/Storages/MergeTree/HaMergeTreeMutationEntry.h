#pragma once

#include <Core/Types.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
class MergeTreeData;

struct HaMergeTreeMutationEntry
{
    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);

    String toString() const;
    String getNameForLogs() const;
    static HaMergeTreeMutationEntry parse(const String & str, String znode_name);

    bool isAlterMutation() const { return alter_version != -1 && !alter_info; }
    bool isAlterMetadata() const { return alter_version != -1 && !!alter_info; }
    void makeAlterMetadata() { alter_info = std::make_unique<AlterMetadataInfo>(); }

    /// return true if rhs has the same query_id and commands with this.
    /// return false if rhs has a different query_id.
    /// throws exception if rhs has the same query_id but different commands.
    bool duplicateWith(const HaMergeTreeMutationEntry & rhs) const;

    /// extract partition id set for commands: FASTDELETE, CLEAR_COLUMN.
    /// make sure that "commands" must be set before using this function.
    /// return false if no part will apply this mutation.
    bool extractPartitionIds(MergeTreeData & storage, ContextPtr context);

    /// return true if partition_id is covered in partition_id set.
    bool coverPartitionId(const String & partition_id) const;

    /// not serialized to znode
    String znode_name;  /// aka mutation id

    /// user could specify a query id for the mutation and later use the id to check status
    String query_id;
    time_t create_time = 0;
    String source_replica;
    Int64 block_number = 0; /// acting as mutation version
    MutationCommands commands;

    /// Record partition_ids when mutation type is FASTDELETE. If partition_ids is empty, it means that user doesn't specify a partition.
    /// The size of partition_ids is at most 1, using set just adapts to previous impl. Previous impl also used this for CLEAR COLUMN and CLEAR COLUMN IN PARTITION WHERE which later may have multiple partition ids.
    NameOrderedSet partition_ids;

    struct AlterMetadataInfo
    {
        bool have_mutation = false;
        String columns_str;
        String metadata_str;
    };

    int alter_version = -1;
    std::unique_ptr<AlterMetadataInfo> alter_info;
};

using HaMergeTreeMutationEntryPtr = std::shared_ptr<const HaMergeTreeMutationEntry>;

}
