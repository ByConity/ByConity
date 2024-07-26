#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * DROP|DETACH|TRUNCATE TABLE [IF EXISTS] [db.]name [PERMANENTLY]
  *
  * Or:
  * DROP DATABASE [IF EXISTS] db
  *
  * Or:
  * DROP DICTIONARY|SNAPSHOT [IF EXISTS] [db.]name
  */
class ParserDropQuery : public IParserDialectBase
{
protected:
    const char * getName() const  override{ return "DROP query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
