#pragma once

#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCommon.h>

namespace DB
{
class Context;

/// Try abort the transaction and return the latest transaction record in kv.
/// Possible status for the returned record are Finished/Aborted/Unknown.
TransactionRecord tryAbortTransactionFromWorker(const Context & context, const TransactionCnchPtr & txn);

}
