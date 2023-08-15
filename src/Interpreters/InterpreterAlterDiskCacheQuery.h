#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTAlterDiskCacheQuery.h>

namespace DB
{
class Context;

class InterpreterAlterDiskCacheQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterAlterDiskCacheQuery(const ASTPtr &, ContextMutablePtr);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};


}
