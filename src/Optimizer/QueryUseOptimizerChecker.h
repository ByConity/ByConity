#pragma once

#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class Context;

class QueryUseOptimizerChecker
{
public:
    static bool check(ASTPtr & node, const ContextMutablePtr & context);
};

struct QueryUseOptimizerContext
{
    const ContextMutablePtr & context;
    NameSet & with_tables;
    Tables external_tables;
};

class QueryUseOptimizerVisitor : public ASTVisitor<bool, QueryUseOptimizerContext>
{
public:
    bool visitNode(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTSelectQuery(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTTableJoin(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTArrayJoin(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTIdentifier(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTFunction(ASTPtr & node, QueryUseOptimizerContext &) override;
    bool visitASTOrderByElement(ASTPtr & node, QueryUseOptimizerContext &) override;

private:
    void collectWithTableNames(ASTSelectQuery & query, NameSet & with_tables);
};

}
