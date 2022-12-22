#pragma once
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

namespace DB
{
class InterpreterDumpInfoQueryUseOptimizer : public IInterpreter
{
public:
    InterpreterDumpInfoQueryUseOptimizer(ASTPtr & query_ptr_, ContextMutablePtr context_)
        : query_ptr(query_ptr_), context(context_), log(&Poco::Logger::get("InterpreterDumpInfoQueryUseOptimizer"))
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    ContextMutablePtr context;
    Poco::Logger * log;
    SelectQueryOptions options;
};
}
