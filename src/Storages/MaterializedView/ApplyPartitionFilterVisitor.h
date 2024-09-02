#pragma once

#include <map>
#include <optional>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST.h>
#include "Parsers/ASTIdentifier.h"

namespace DB
{
class ASTFunction;
class ASTSelectQuery;
class ASTSelectWithUnionQuery;
struct ASTTableExpression;

class ApplyPartitionFilterVisitor
{
public:
    struct Data
    {
        std::map<String, ASTPtr> subqueries;

        StorageID target_storage_id;
        ASTPtr target_filter;
        ContextPtr context;
    };

    static void visit(ASTPtr & ast, StorageID target_storage_id, ASTPtr target_filter, ContextPtr context);
    static void visit(ASTSelectQuery & ast, StorageID target_storage_id, ASTPtr target_filter, ContextPtr context);
    static void visit(ASTSelectWithUnionQuery & ast, StorageID target_storage_id, ASTPtr target_filter, ContextPtr context);

private:
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
    // static void visit(ASTIdentifier & identifier, const Data & data);

    static ASTPtr constructSubquery(const ASTPtr & table_identifier, const ASTPtr & filter, const String & alias);
};

}
