#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

/// Returns single row with explain results
class InterpreterExplainQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterExplainQuery(const ASTPtr & query_, ContextMutablePtr context_) : WithMutableContext(context_),
        query(query_), log(&Poco::Logger::get("InterpreterExplainQuery")) {}

    BlockIO execute() override;

    Block getSampleBlock();

private:
    ASTPtr query;
    Poco::Logger * log;

    BlockInputStreamPtr executeImpl();

    void rewriteDistributedToLocal(ASTPtr & ast);

    void elementDatabaseAndTable(const ASTSelectQuery & select_query, const ASTPtr & where, WriteBuffer & buffer);

    void elementWhere(const ASTPtr & where, WriteBuffer & buffer);

    void elementDimensions(const ASTPtr & select, WriteBuffer & buffer);

    void elementMetrics(const ASTPtr & select, WriteBuffer & buffer);

    void elementGroupBy(const ASTPtr & group_by, WriteBuffer & buffer);

    void listPartitionKeys(StoragePtr & storage, WriteBuffer & buffer);

    void listRowsOfOnePartition(StoragePtr & storage, const ASTPtr & group_by, const ASTPtr & where, WriteBuffer & buffer);

    std::optional<String> getActivePartCondition(StoragePtr & storage);

    void explainUsingOptimizer(const ASTPtr & ast, WriteBuffer & buffer, bool & single_line);
};


}
