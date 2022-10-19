#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Statistics/StatisticsCommon.h>

namespace DB
{
class Context;

class InterpreterShowStatsQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowStatsQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;


private:
    BlockIO executeTable();
    BlockIO executeColumn();
    void executeSpecial();

    ASTPtr query_ptr;
};

}
