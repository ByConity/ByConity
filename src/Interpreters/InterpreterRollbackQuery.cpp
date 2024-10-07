#include <Interpreters/Context.h>
#include <Interpreters/InterpreterRollbackQuery.h>
#include <Transaction/CnchExplicitTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SESSION_NOT_FOUND;
    extern const int SUPPORT_IS_DISABLED;
}

BlockIO InterpreterRollbackQuery::execute()
{
    if (!getContext()->getSessionContext()->getSettingsRef().enable_interactive_transaction)
    {
        throw Exception(
            "Interactive transactions are disabled, can enable the feature by 'SET enable_interactive_transaction=1'",
            ErrorCodes::SUPPORT_IS_DISABLED);
    }

    auto session_context = getContext()->getSessionContext();
    auto txn = session_context->getCurrentTransaction();
    auto & coordinator = session_context->getCnchTransactionCoordinator();


    if (!txn)
        throw Exception("No available active explicit transaction in current session", ErrorCodes::LOGICAL_ERROR);

    auto explicit_txn = std::dynamic_pointer_cast<CnchExplicitTransaction>(txn);

    if (!explicit_txn)
    {
        throw Exception("Transaction " + txn->getTransactionID().toString() + " is not explicit transaction", ErrorCodes::LOGICAL_ERROR);
    }

    SCOPE_EXIT(session_context->setCurrentTransaction(nullptr););
    auto log = getLogger("InterpreterRollbackQuery");

    LOG_DEBUG(log, "Rollbacking explicit transaction: {}", explicit_txn->getTransactionID());
    try
    {
        explicit_txn->abort();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to rollback explicit transaction {}", txn->getTransactionID());
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
    LOG_INFO(log, "Rollbacked explicit transaction: {}", txn->getTransactionID());
    coordinator.finishTransaction(explicit_txn);

    return {};
}

} // end of namespace DB
