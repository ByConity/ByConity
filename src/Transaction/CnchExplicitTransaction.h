#pragma once
#include <Transaction/TxnTimestamp.h>
#include <Interpreters/Context.h>
#include <Common/HostWithPorts.h>
#include <Transaction/Actions/IAction.h>
#include <Transaction/ICnchTransaction.h>
#include <Common/CurrentMetrics.h>

namespace DB
{
    class CnchExplicitTransaction : public ICnchTransaction
    {
    friend class TransactionCoordinatorRcCnch;
    using Base = ICnchTransaction;
    private:
        Poco::Logger * log {&Poco::Logger::get("CnchExplicitTransaction")};
        std::vector<TransactionCnchPtr> secondary_txns;
        std::vector<String> statements;
        static constexpr int MAX_RETRY = 3; 
    public:
        CnchExplicitTransaction(const ContextPtr & context, TransactionRecord record);
        ~CnchExplicitTransaction() override = default;

        String getTxnType() const override { return "CnchExplicitTransaction"; }

        void precommit() override;
        TxnTimestamp commit() override;
        TxnTimestamp commitV2() override;
        TxnTimestamp rollback() override;
        TxnTimestamp abort() override;
        void clean(TxnCleanTask & task) override;
        bool addSecondaryTransaction(const TransactionCnchPtr & txn);
        bool addStatement(const String & statement);
        const std::vector<String> & getStatements() const { return statements; }
        const std::vector<TransactionCnchPtr> & getSecondaryTransactions() const { return secondary_txns; }
    };
    using CnchExplicitTransactionPtr = std::shared_ptr<CnchExplicitTransaction>;
}
