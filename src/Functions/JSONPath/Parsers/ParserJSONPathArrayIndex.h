#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
class ParserJSONPathArrayIndex : public IParserBase
{
    const char * getName() const override { return "ParserJSONPathArrayIndex"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
