#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query Switch catalog
  */
class ParserSwitchQuery : public IParserBase
{
protected:
    const char * getName() const override { return "Switch query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
