#pragma once

#include <Common/Logger.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>

namespace DB
{
class Context;

class InterpreterAutoStatsQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterAutoStatsQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    LoggerPtr log = getLogger("InterpreterAutoStats");
};

}
