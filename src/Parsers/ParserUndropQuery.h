#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/**
  * DROP|DETACH|TRUNCATE TABLE [IF EXISTS] [db.]name
  * DROP DATABASE [IF EXISTS] db
  */
class ParserUndropQuery : public IParserBase
{
protected:
    const char * getName() const override { return "UNDROP query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
