#pragma once

#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserQueryWithOutput.h>

namespace DB
{
/** Query Dump explainable_statement
 * Syntax is as follows:
 * Dump query
 */
class ParserDumpInfoQuery : public IParserDialectBase
{
protected:
    const char * end;
    const char * getName() const override { return "Dump query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    ParserDumpInfoQuery(const char * end_, ParserSettingsImpl t)
        : IParserDialectBase(t), end(end_)
    {
    }
};

}
