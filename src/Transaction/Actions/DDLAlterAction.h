#pragma once

#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MutationCommands.h>
#include <Transaction/Actions/Action.h>

namespace DB
{

class DDLAlterAction : public Action
{
public:
    DDLAlterAction(const Context & context_, const TxnTimestamp & txn_id_, StoragePtr table_)
        : Action(context_, txn_id_),
        log(&Poco::Logger::get("AlterAction")),
        table(std::move(table_))
    {
    }

    ~DDLAlterAction() override = default;

    /// TODO: versions
    void setNewSchema(String schema_);
    String getNewSchema() const { return new_schema; }

    void setMutationCommands(MutationCommands commands);

    void executeV1(TxnTimestamp commit_time) override;

private:
    void updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time) override;
    void appendPart(MutableMergeTreeDataPartCNCHPtr part);
    static void updatePartData(MutableMergeTreeDataPartCNCHPtr part, TxnTimestamp commit_time);

    Poco::Logger * log;
    const StoragePtr table;

    String new_schema;
    MutationCommands mutation_commands;
};

using DDLAlterActionPtr = std::shared_ptr<DDLAlterAction>;

}
