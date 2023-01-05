#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

struct ASTTableExpression;
class ASTIdentifier;
class ASTQualifiedAsterisk;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

class RewriteDistributedQueryMatcher
{
public:

    struct Data
    {
        std::unordered_map<IAST*, std::pair<String, String>> table_rewrite_info;
        std::vector<std::pair<DatabaseAndTableWithAlias, String>> identifier_rewrite_info;
        ClusterPtr cluster;
        String cluster_name;
        bool all_distributed = true;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

    static void visit(ASTPtr & ast, Data & data);

    static StoragePtr tryGetTable(const ASTPtr & database_and_table, const ContextPtr & context);

    static Data collectTableInfos(const ASTPtr & query, const ContextPtr & context);

private:

    static void visit(ASTTableExpression & query, ASTPtr & node, Data & data);
    static void visit(ASTIdentifier & query, ASTPtr & node, Data & data);
    static void visit(ASTQualifiedAsterisk & query, ASTPtr & node, Data & data);
};

using RewriteDistributedQueryVisitor = InDepthNodeVisitor<RewriteDistributedQueryMatcher, true>; 

}
