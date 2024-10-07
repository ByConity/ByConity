#pragma once
#include <Common/Logger.h>
#include <Transaction/TableTxnCommitter.h>

namespace DB
{

class GlobalTxnCommitter : public WithContext
{

public:
    explicit GlobalTxnCommitter(const ContextPtr & context_);

    bool commit(const TransactionCnchPtr & txn);

private:

    TableTxnCommitterPtr getTableTxnCommitter(const UUID & uuid);

    std::mutex committers_mutex;
    std::map<UUID, TableTxnCommitterPtr> committers;

    LoggerPtr log{getLogger("GlobalTXNComitter")};
};

using GlobalTxnCommitterPtr = std::shared_ptr<GlobalTxnCommitter>;
}
