#pragma once

#include <memory>

#include <Core/QueryProcessingStage.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

struct ASTTableExpression;
class ASTIdentifier;
class ASTQualifiedAsterisk;

class RewriteDistributedTableMatcher
{
public:

    struct Data
    {
        std::unordered_map<IAST*, std::pair<String, String>> table_rewrite_info;
        std::vector<std::pair<DatabaseAndTableWithAlias, String>> identifier_rewrite_info;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

    static void visit(ASTPtr & ast, Data & data);

private:

    static void visit(ASTTableExpression & query, ASTPtr & node, Data & data);
    static void visit(ASTIdentifier & query, ASTPtr & node, Data & data);
    static void visit(ASTQualifiedAsterisk & query, ASTPtr & node, Data & data);
};

using RewriteDistributedTableVisitor = InDepthNodeVisitor<RewriteDistributedTableMatcher, true>; 


class InterpreterPerfectShard
{
public:
    InterpreterPerfectShard(InterpreterSelectQuery & interpreter_)
    : interpreter(interpreter_)
    , query(interpreter.query_ptr)
    , context(interpreter.context)
    , log(&Poco::Logger::get("InterpreterPerfectShard"))
    {
        query_info.query = query;
        collectTables();
    }

    void buildQueryPlan(QueryPlan & query_plan);

    bool checkPerfectShardable();

private:

    void collectTables();
    void rewriteDistributedTables();
    QueryProcessingStage::Enum determineProcessingStage();
    void sendQuery(QueryPlan & query_plan);
    void buildFinalPlan(QueryPlan & query_plan);
    void addAggregation(QueryPlan & query_plan);

    void getOriginalProject();

    InterpreterSelectQuery & interpreter;
    ASTPtr query;
    std::shared_ptr<Context> context;
    Poco::Logger * log;

    SelectQueryInfo query_info;
    bool perfect_shardable = true;

    std::unordered_map<IAST*, std::pair<String, String>> table_rewrite_info;
    std::vector<std::pair<DatabaseAndTableWithAlias, String>> identifier_rewrite_info;

    String main_table;
    String main_database;

    QueryProcessingStage::Enum processed_stage;
    std::unordered_map<String, String> original_project;
};

}
