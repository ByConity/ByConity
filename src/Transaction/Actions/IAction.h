#pragma once

#include <Core/Types.h>
// #include <Databases/DatabaseCnch.h>
#include <Storages/IStorage_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class Context;

class IAction : public WithContext
{
public:
    IAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_);
    virtual ~IAction() = default;

    /// V1 is the old API which performs data write and txn commit in one api calls.
    /// V2 is the new API which separate data write and txn commit with 2 api calls.
    /// Currently, still keep both of them as ddl is still executed with old api because the db/table metadata still does not support intermediate state.
    virtual void executeV1(TxnTimestamp commit_time) = 0; // write data and commit.
    virtual void executeV2() {} // write data only, do not commit here.
    virtual void abort() {}
    virtual void postCommit(TxnTimestamp /*commit_time*/) {}

    virtual UInt32 collectNewParts() const { return 0; }
    virtual UInt32 getSize() const { return 0; }
protected:
    const Context & global_context;
    TxnTimestamp txn_id;

private:
    virtual void updateTsCache(const UUID &, const TxnTimestamp &) {}
};

using ActionPtr = std::shared_ptr<IAction>;

}
