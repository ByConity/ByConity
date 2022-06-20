#pragma once

#include <Core/UUID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Transaction/Actions/Action.h>

namespace DB
{
struct CreateActionParams
{
    String database;
    String table;
    UUID uuid;
    String statement;
};

class DDLCreateAction : public Action
{

public:
    DDLCreateAction(const Context & context_, const TxnTimestamp & txn_id_, CreateActionParams params_)
        : Action(context_, txn_id_), params(std::move(params_))
    {}

    ~DDLCreateAction() override = default;

    void executeV1(TxnTimestamp commit_time) override;
    void abort() override;

private:
    void updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time) override;

private:
    CreateActionParams params;
};

using DDLCreateActionPtr = std::shared_ptr<DDLCreateAction>;

}



