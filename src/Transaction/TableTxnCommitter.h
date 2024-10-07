#pragma once
#include <Common/Logger.h>
#include <Transaction/CnchServerTransaction.h>


namespace DB
{

class TableTxnCommitter
{

public:
    explicit TableTxnCommitter(const ContextPtr & context_, const UUID & storage_uuid_)
        : context(context_),
          uuid(storage_uuid_),
          log(getLogger("TnxComitter(" + UUIDHelpers::UUIDToString(uuid) + ")"))
    {
    }

    bool commit(const TransactionCnchPtr & txn);

private:

    UInt64 getNewVersion();
    UInt64 getTableVersion();


    ContextPtr context;
    const UUID uuid;
    LoggerPtr log;
    std::atomic<UInt64> latest_version {0};

    std::mutex commit_mutex;
};


using TableTxnCommitterPtr = std::shared_ptr<TableTxnCommitter>;

}
