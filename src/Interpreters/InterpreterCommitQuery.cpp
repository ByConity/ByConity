#include <Interpreters/InterpreterCommitQuery.h>

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

BlockIO InterpreterCommitQuery::execute()
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
        throw Exception("No available active transaction in current session.", ErrorCodes::LOGICAL_ERROR);

    auto explicit_txn = std::dynamic_pointer_cast<CnchExplicitTransaction>(txn);
    if (!explicit_txn)
        throw Exception("Session transaction is not explicit transaction.", ErrorCodes::LOGICAL_ERROR);

    SCOPE_EXIT(session_context->setCurrentTransaction(nullptr););

    auto log = getLogger("InterpreterCommitQuery");

    LOG_INFO(log, "Committing explicit transaction: {}", explicit_txn->getTransactionID());
    try
    {
        coordinator.commitV2(txn);
    }
    catch (...)
    {
        /// rollback logic is implemented inside the coordinator
        LOG_ERROR(log, "Failed to commit explicit transaction {}", txn->getTransactionID());
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
    LOG_INFO(log, "Committed transaction: {}", explicit_txn->getTransactionID());

    return {};
}

} // end of namespace DB
