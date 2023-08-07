#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTTransaction.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class Context;

class InterpreterRollbackQuery : public IInterpreter, WithContext
{
public:
    InterpreterRollbackQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(std::move(context_)), query_ptr(query_ptr_) { }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

} // end of namespace
