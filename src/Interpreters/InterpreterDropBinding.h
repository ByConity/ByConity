#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class InterpreterDropBinding : public IInterpreter
{
public:
    InterpreterDropBinding(const ASTPtr & query_ptr_, ContextMutablePtr & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    ContextMutablePtr context;
};

}
