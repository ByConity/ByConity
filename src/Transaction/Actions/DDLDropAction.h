#pragma once

#include <Parsers/ASTDropQuery.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Transaction/Actions/Action.h>

namespace DB
{
struct DropActionParams
{
    String database;
    String table;
    TxnTimestamp prev_version;
    ASTDropQuery::Kind kind;
};

class DDLDropAction : public Action
{
public:
    DDLDropAction(const Context & context_, const TxnTimestamp & txn_id_, DropActionParams params_, std::vector<StoragePtr> tables_ = {})
        : Action(context_, txn_id_), params(std::move(params_)), tables(std::move(tables_)), log(&Poco::Logger::get("DropAction"))
    {
    }

    ~DDLDropAction() override = default;

    void executeV1(TxnTimestamp commit_time) override;

private:
    void dropDataParts(const MergeTreeDataPartsCNCHVector & parts);
    void updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time) override;

private:
    DropActionParams params;
    std::vector<StoragePtr> tables;
    Poco::Logger * log;
};

using DDLDropActionPtr = std::shared_ptr<DDLDropAction>;

}
