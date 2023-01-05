
#pragma once
#include <Interpreters/IInterpreter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTDropWorkerGroupQuery.h>

namespace DB
{
class Context;

class InterpreterDropWorkerGroupQuery : public IInterpreter, WithContext
{
public:
    InterpreterDropWorkerGroupQuery(const ASTPtr & query_ptr, ContextPtr context_);
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};
}
