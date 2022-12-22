#pragma once
#include <Interpreters/IInterpreter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTCreateWorkerGroupQuery.h>

namespace DB
{
class Context;

class InterpreterCreateWorkerGroupQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateWorkerGroupQuery(const ASTPtr & query_ptr, ContextPtr context_);
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};
}
