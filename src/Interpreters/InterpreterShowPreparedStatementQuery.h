#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

#include <utility>

namespace DB
{
class InterpreterShowPreparedStatementQuery : public IInterpreter
{
public:
    explicit InterpreterShowPreparedStatementQuery(const ASTPtr & query_ptr_, ContextMutablePtr & context_)
        : query_ptr(query_ptr_), context(context_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    ContextMutablePtr context;
};
}
