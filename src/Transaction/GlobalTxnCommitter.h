#pragma once
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

    Poco::Logger * log{&Poco::Logger::get("GlobalTXNComitter")};
};

using GlobalTxnCommitterPtr = std::shared_ptr<GlobalTxnCommitter>;
}
