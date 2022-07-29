#include "IAction.h"
#include <Interpreters/Context_fwd.h>

namespace DB
{

IAction::IAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_)
    : WithContext(query_context_), txn_id(txn_id_)
{
}

}
