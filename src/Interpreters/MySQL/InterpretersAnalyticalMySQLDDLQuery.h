#pragma once

#include <Common/Logger.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/MySQL/ASTAlterQuery.h>
#include <Parsers/ASTCreateQueryAnalyticalMySQL.h>
#include <Parsers/queryToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ASSIGN_ALTER;
}

namespace MySQLInterpreter
{
    struct InterpreterAlterAnalyticalMySQLImpl
    {
        using TQuery = ASTAlterAnalyticalMySQLQuery;

        static void validate(const TQuery & query, ContextPtr context);

        static ASTPtr getRewrittenQuery(const TQuery & alter_query, ContextPtr context);
    };

    struct InterpreterCreateAnalyticMySQLImpl
    {
        using TQuery = ASTCreateQueryAnalyticalMySQL;

        static void validate(const TQuery & query, ContextPtr context);

        static ASTPtr getRewrittenQuery(const TQuery & create_query, ContextPtr context);
    };

template <typename InterpreterImpl>
class InterpreterAnalyticalMySQLDDLQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterAnalyticalMySQLDDLQuery(
        const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_), log(getLogger("InterpreterAnalyticalMySQLDDLQuery"))
    {
    }

    BlockIO execute() override
    {
        const typename InterpreterImpl::TQuery & query = query_ptr->as<typename InterpreterImpl::TQuery &>();

        LOG_DEBUG(log, "Begin to rewritte mysql query {}", queryToString(query));
        InterpreterImpl::validate(query, getContext());
        ASTPtr rewritten_query = InterpreterImpl::getRewrittenQuery(query, getContext());

        // only one rewriten query every time
        if (rewritten_query)
        {
            LOG_DEBUG(log, "Execute rewritten mysql query {}", queryToString(rewritten_query));
            /// Set `internal` to true as we use CnchStorageCommonHelper to forward query to target server when needed
            return executeQuery("/* Rewritten MySQL DDL Query */ " + queryToString(rewritten_query), rewritten_query, getContext(), true);
        }

        return BlockIO{};
    }

private:
    ASTPtr query_ptr;
    LoggerPtr log;
};

using InterpreterAnalyticalMySQLAlterQuery = InterpreterAnalyticalMySQLDDLQuery<InterpreterAlterAnalyticalMySQLImpl>;
using InterpreterAnalyticalMySQLCreateQuery = InterpreterAnalyticalMySQLDDLQuery<InterpreterCreateAnalyticMySQLImpl>;

}

}
