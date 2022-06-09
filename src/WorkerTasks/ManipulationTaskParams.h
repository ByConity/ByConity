#pragma once

#include <WorkerTasks/ManipulationType.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MutationCommands.h>
// #include <Transaction/TxnTimestamp.h>
#include <Core/Types.h>
// #include <CatalogService/DataModelPartWrapper_fwd.h>

namespace DB
{

class MutationCommands;

struct ManipulationTaskParams
{
    using Type = ManipulationType;

    Type type{Type::Empty};
    UInt16 rpc_port{0};

    String task_id;
    Int64 txn_id{0};

    String create_table_query; /// non-empty if the task needs to create temporary table
    StoragePtr storage; /// On which storage this manipulate task should run
    bool is_bucket_table{false}; /// mark if the table is treated as bucket table when create this manipulation task

    MergeTreeDataPartsVector all_parts; /// Used by callee
    // ServerDataPartsVector source_parts;
    MergeTreeDataPartsVector source_data_parts; // Used by worker
    Strings new_part_names;

    UInt64 columns_commit_time{0};
    UInt64 mutation_commit_time{0};

    std::shared_ptr<MutationCommands> mutation_commands;

    ManipulationTaskParams(StoragePtr s) : storage(std::move(s)) { }
    ManipulationTaskParams(const ManipulationTaskParams &) = default;
    ManipulationTaskParams & operator=(const ManipulationTaskParams &) = default;
    ManipulationTaskParams(ManipulationTaskParams &&) = default;
    ManipulationTaskParams & operator=(ManipulationTaskParams &&) = default;

    String toDebugString() const;

    void assignSourceParts(MergeTreeDataPartsVector source_parts);
};

}
