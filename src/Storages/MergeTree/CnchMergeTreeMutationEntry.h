#pragma once

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Storages/MutationCommands.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/Exception.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

struct CnchMergeTreeMutationEntry
{
    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);

    String toString() const;
    static CnchMergeTreeMutationEntry parse(const String & str);
    bool isReclusterMutation() const;

    TxnTimestamp txn_id; // mock
    TxnTimestamp commit_time;

    /*
     * for BUILD_BITMAP, we wouldn't update storage version in CnchMergeTree::alter.
     * so in this case, we use columns_commit_time to judge whether we should execute MutationCommands for parts.
     * */

    TxnTimestamp columns_commit_time;

    /// Mutation commands which will give to MUTATE_PART entries
    MutationCommands commands;
};

}
