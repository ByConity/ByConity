#pragma once

#include <Core/UUID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Transaction/Actions/IAction.h>

namespace DB
{
struct CreateActionParams
{
    String database;
    String table;
    UUID uuid;
    String statement;
    bool attach = false;
    bool is_dictionary = false;
};

class DDLCreateAction : public IAction
{

public:
    DDLCreateAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_, CreateActionParams params_)
        : IAction(query_context_, txn_id_), params(std::move(params_))
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



