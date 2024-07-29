#include <Transaction/TableTxnCommitter.h>
#include <Transaction/CnchServerTransaction.h>
#include <Catalog/Catalog.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
}

/// TODO: more efficient way to generate new table version.
UInt64 TableTxnCommitter::getNewVersion()
{
    latest_version = context->getTimestamp();
    return latest_version;
}

UInt64 TableTxnCommitter::getTableVersion()
{
    return latest_version;
}

bool TableTxnCommitter::commit(const TransactionCnchPtr & txn)
{
    std::lock_guard<std::mutex> lock(commit_mutex);
    auto res = context->getCnchCatalog()->commitTransactionWithNewTableVersion(txn, getNewVersion());
    LOG_DEBUG(log, "Committed transaction {} with new version {} of table {}", txn->getTransactionID().toString(),
        getTableVersion(), UUIDHelpers::UUIDToString(uuid));
    return res;
}

}
