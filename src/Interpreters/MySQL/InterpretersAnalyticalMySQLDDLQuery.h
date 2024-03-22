#pragma once

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

namespace MySQLInterpreter
{
    // struct InterpreterDropImpl
    // {
    //     using TQuery = ASTDropQuery;

    //     static void validate(const TQuery & query, ContextPtr context);

    //     static ASTs getRewrittenQueries(
    //         const TQuery & drop_query, ContextPtr context, const String & mapped_to_database, const String & mysql_database);
    // };

    struct InterpreterAlterAnalyticalMySQLImpl
    {
        using TQuery = ASTAlterAnalyticalMySQLQuery;

        static void validate(const TQuery & query, ContextPtr context);

        static ASTs getRewrittenQueries(const TQuery & alter_query, ContextPtr context);
    };

    // struct InterpreterRenameImpl
    // {
    //     using TQuery = ASTRenameQuery;

    //     static void validate(const TQuery & query, ContextPtr context);

    //     static ASTs getRewrittenQueries(
    //         const TQuery & rename_query, ContextPtr context, const String & mapped_to_database, const String & mysql_database);
    // };

    struct InterpreterCreateAnalyticMySQLImpl
    {
        using TQuery = ASTCreateQueryAnalyticalMySQL;

        static void validate(const TQuery & query, ContextPtr context);

        static ASTs getRewrittenQueries(
            const TQuery & create_query, ContextPtr context);
    };

template <typename InterpreterImpl>
class InterpreterAnalyticalMySQLDDLQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterAnalyticalMySQLDDLQuery(
        const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_), log(&Poco::Logger::get("InterpreterAnalyticalMySQLDDLQuery"))
    {
    }

    BlockIO execute() override
    {
        const typename InterpreterImpl::TQuery & query = query_ptr->as<typename InterpreterImpl::TQuery &>();

        LOG_DEBUG(log, "Begin to rewritte mysql query {}", queryToString(query));
        InterpreterImpl::validate(query, getContext());
        ASTs rewritten_queries = InterpreterImpl::getRewrittenQueries(query, getContext());

        for (const auto & rewritten_query : rewritten_queries)
        {
            LOG_DEBUG(log, "Execute rewritten mysql query {}", queryToString(rewritten_query));
            /// Set `internal` to true as we use CnchStorageCommonHelper to forward query to target server when needed
            executeQuery("/* Rewritten MySQL DDL Query */ " + queryToString(rewritten_query), rewritten_query, getContext(), true);
        }

        return BlockIO{};
    }

private:
    ASTPtr query_ptr;
    Poco::Logger * log;
};

// using InterpreterAnalyticalMySQLDropQuery = InterpreterAnalyticalMySQLDDLQuery<InterpreterDropImpl>;
using InterpreterAnalyticalMySQLAlterQuery = InterpreterAnalyticalMySQLDDLQuery<InterpreterAlterAnalyticalMySQLImpl>;
// using InterpreterAnalyticalMySQLRenameQuery = InterpreterAnalyticalMySQLDDLQuery<InterpreterRenameImpl>;
using InterpreterAnalyticalMySQLCreateQuery = InterpreterAnalyticalMySQLDDLQuery<InterpreterCreateAnalyticMySQLImpl>;

}

}
