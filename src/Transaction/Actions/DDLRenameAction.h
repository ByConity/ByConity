#pragma once

#include <Core/UUID.h>
#include <Parsers/ASTRenameQuery.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Transaction/Actions/IAction.h>

namespace DB
{

struct RenameActionParams
{
    struct RenameTableParams
    {
        String from_database;
        String from_table;
        UUID from_table_uuid;
        String to_database;
        String to_table;
    };

    struct RenameDBParams
    {
        String from_database;
        String to_database;
        std::vector<UUID> uuids;
    };

    enum class Type
    {
        RENAME_DB,
        RENAME_TABLE
    };

    RenameTableParams table_params{};
    RenameDBParams db_params{};
    Type type {Type::RENAME_TABLE};
};


class DDLRenameAction : public IAction
{
public:
    DDLRenameAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_, RenameActionParams params_)
        : IAction(query_context_, txn_id_), params(std::move(params_))
    {}

    ~DDLRenameAction() override = default;

    void executeV1(TxnTimestamp commit_time) override;

private:
    void updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time) override;

    void renameTablePrefix(TxnTimestamp commit_time);
    void renameTableSuffix(TxnTimestamp commit_time);

private:
    RenameActionParams params;

    bool is_cnch_merge_tree{false};
    // bool is_cnch_kafka{false};
};

using DDLRenameActionPtr = std::shared_ptr<DDLRenameAction>;

}
