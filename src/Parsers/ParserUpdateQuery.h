#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

// parse ASTUpdateQuery
class ParserUpdateQuery : public IParserDialectBase
{
protected:
    const char * getName() const  override{ return "Update query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
