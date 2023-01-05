#pragma once

#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserQueryWithOutput.h>

namespace DB
{
/** Query Reproduce
 * Syntax is as follows:
 * Reproduce /path/of/query_id.zip
 */
class ParserReproduceQuery : public IParserDialectBase
{
protected:
    const char * end;
    const char * getName() const override{ return "REPRODUCE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    ParserReproduceQuery(const char * end_) : end(end_) { }
};
}
