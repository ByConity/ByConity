#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Statistics/StatisticsCommon.h>
#include <Statistics/StatsTableIdentifier.h>

namespace DB
{
class InterpreterCreateStatsQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateStatsQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
