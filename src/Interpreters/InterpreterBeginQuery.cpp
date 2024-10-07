#include <Interpreters/InterpreterBeginQuery.h>

#include <Interpreters/Context.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SESSION_NOT_FOUND;
    extern const int SUPPORT_IS_DISABLED;
}
BlockIO InterpreterBeginQuery::execute()
{
    if (!getContext()->getSessionContext()->getSettingsRef().enable_interactive_transaction)
    {
        throw Exception(
            "Interactive transactions are disabled, can enable the feature by 'SET enable_interactive_transaction=1'",
            ErrorCodes::SUPPORT_IS_DISABLED);
    }
    if (!getContext()->hasSessionContext())
    {
        throw Exception("Interactive transaction must have session", ErrorCodes::SESSION_NOT_FOUND);
    }
    auto session_context = getContext()->getSessionContext();

    if (session_context->getCurrentTransaction())
    {
        throw Exception(
            "A transaction has already began: " + session_context->getCurrentTransaction()->getTransactionID().toString(),
            ErrorCodes::LOGICAL_ERROR);
    }
    LOG_INFO(getLogger("InterpreterBeginQuery"), "Creating new explicit transaction");
    auto & coordinator = session_context->getCnchTransactionCoordinator();
    auto txn = coordinator.createTransaction(CreateTransactionOption().setType(CnchTransactionType::Explicit));
    if (txn)
    {
        /// txn_id is not specified, so this is the coordinator and we set session transaction to transaction
        session_context->setCurrentTransaction(txn);
    }
    else
    {
        throw Exception("Failed to create new explicit transaction", ErrorCodes::LOGICAL_ERROR);
    }

    LOG_INFO(getLogger("InterpreterBeginQuery"), "Begin a new explicit transaction: {}", txn->getTransactionID());

    return {};
}

} // end of namespace DB
