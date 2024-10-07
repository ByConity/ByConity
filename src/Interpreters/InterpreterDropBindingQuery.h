#pragma once

#include <Common/Logger.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class InterpreterDropBindingQuery : public IInterpreter
{
public:
    InterpreterDropBindingQuery(const ASTPtr & query_ptr_, ContextMutablePtr & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    ContextMutablePtr context;
    LoggerPtr log = getLogger("InterpreterDropBindingQuery");
};

}
