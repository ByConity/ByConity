#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/**
 * TEALIMIT N GROUP g1 ... gn ORDER expr(...) ASC|DESC clause
 */
class ParserTEALimitClause : public IParserBase
{
    protected:
        const char * getName() const override { return "TEALIMIT clause"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

