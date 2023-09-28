#pragma once

#include <DataStreams/BlockIO.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/JSON/Object.h>
#include <Poco/Logger.h>
#include <QueryPlan/QueryPlan.h>

#include <string>

// helpers for reproducing queries
namespace DB::ReproduceUtils
{
/**
 * @enum defines the status of databases/tables in reproducer
 */
enum class DDLStatus
{
    failed = 0,
    created = 1,
    reused = 2,
    dropped = 3,
    created_and_loaded_stats = 4,
    reused_and_loaded_stats = 5,
    unknown = 6,
};

const char * toString(DDLStatus stats);
ASTPtr parse(const std::string & query, ContextPtr query_context);
void executeDDL(ConstASTPtr query, ContextMutablePtr query_context);
std::string obtainExplainString(const std::string & select_query, ContextMutablePtr query_context);
std::string getFolder(const std::string & file_path);
Poco::JSON::Object::Ptr readJsonFromAbsolutePath(const std::string & absolute_path);
}

