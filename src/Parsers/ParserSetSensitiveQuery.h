#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * SET SENSITIVE DATABASE db = true;
  * SET SENSITIVE TABLE db.tb = false;
  * SET SENSITIVE COLUMN db.tb(col_1) = true;
  */
class ParserSetSensitiveQuery : public IParserBase
{
public:
    explicit ParserSetSensitiveQuery() {}
protected:
    const char * getName() const override { return "SET SENSITIVE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
