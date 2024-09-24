#include <Transaction/GlobalTxnCommitter.h>
#include <MergeTreeCommon/CnchServerManager.h>
#include <CloudServices/CnchServerClientPool.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    // extern const int BAD_CAST;
}

GlobalTxnCommitter::GlobalTxnCommitter(const ContextPtr & context_)
    : WithContext(context_->getGlobalContext())
{}

TableTxnCommitterPtr GlobalTxnCommitter::getTableTxnCommitter(const UUID & uuid)
{
    TableTxnCommitterPtr table_txn_commiter;
    std::unique_lock<std::mutex> lock(committers_mutex);
    auto it = committers.find(uuid);
    if (it == committers.end())
    {
        table_txn_commiter = std::make_shared<TableTxnCommitter>(getContext(), uuid);
        committers.emplace(uuid, table_txn_commiter);
    }
    else
        table_txn_commiter = it->second;
    return table_txn_commiter;
}

bool GlobalTxnCommitter::commit(const TransactionCnchPtr & txn)
{
    auto server_manager = getContext()->getCnchServerManager();
    if (!server_manager)
        throw Exception("Cannot commit transaction because ServerManager is unavailable.", ErrorCodes::LOGICAL_ERROR);

    if (!server_manager->isLeader())
    {
        auto current_leader = server_manager->getCurrentLeader();
        if (!current_leader)
            throw Exception("Cannot commit transaction since leader is unavailable.", ErrorCodes::LOGICAL_ERROR);
        getContext()->getCnchServerClientPool().get(*current_leader)->commitTransactionViaGlobalCommitter(txn);

        return true;
    }
    else
    {
        auto committer = getTableTxnCommitter(txn->getMainTableUUID());
        return committer->commit(txn);
    }
}

}
