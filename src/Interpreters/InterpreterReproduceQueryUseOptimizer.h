#pragma once
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

namespace DB
{
class InterpreterReproduceQueryUseOptimizer : public IInterpreter
{
public:
    InterpreterReproduceQueryUseOptimizer(ASTPtr & query_ptr_, ContextMutablePtr & context_)
        : query_ptr(query_ptr_), context(context_), log(&Poco::Logger::get("InterpreterReproduceQueryUseOptimizer"))
    {
    }

    ASTPtr parse(const String & query);
    QueryPlanPtr plan(ASTPtr ast);
    void createTablesFromJson(const String & path);
    void createClusterInfo(const String & path);
    void resetTransaction();

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    ContextMutablePtr context;
    SelectQueryOptions options;
    Poco::Logger * log;
};

}
