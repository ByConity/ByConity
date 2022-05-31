#pragma once

#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MutationCommands.h>
#include <Transaction/Actions/Action.h>

namespace DB
{

class DDLAlterAction : public Action
{
public:
    DDLAlterAction(const Context & context_, const TxnTimestamp & txn_id_, const StoragePtr table_)
    : Action(context_, txn_id_),
    table(table_),
    log(&Poco::Logger::get("AlterAction"))
    {
    }

    ~DDLAlterAction() override = default;

    /// TODO: versions
    void setNewSchema(String schema_);
    String getNewSchema() const { return new_schema; }

    void setMutationCommmands(MutationCommands commands);

    void executeV1(TxnTimestamp commit_time) override;

private:
    void updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time) override;
    void appendPart(MutableMergeTreeDataPartCNCHPtr part);
    void updatePartData(MutableMergeTreeDataPartCNCHPtr part, TxnTimestamp commit_time);

private:
    const StoragePtr table;
    Poco::Logger * log;

    String new_schema;
    MutationCommands mutation_commands;
};

using DDLAlterActionPtr = std::shared_ptr<DDLAlterAction>;

}

