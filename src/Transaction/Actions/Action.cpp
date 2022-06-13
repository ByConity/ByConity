#include "Action.h"

namespace DB
{

Action::Action(const Context & context_, const TxnTimestamp & txn_id_)
    : context(context_), txn_id(txn_id_)
{
}

}
