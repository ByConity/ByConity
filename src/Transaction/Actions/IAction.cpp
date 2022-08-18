#include "IAction.h"
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Context.h>

namespace DB
{

IAction::IAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_)
    : WithContext(query_context_), global_context(*query_context_->getGlobalContext()), txn_id(txn_id_)
{
}

}
