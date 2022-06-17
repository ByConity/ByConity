#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries like
 *  DROP WAREHOUSE name
 */
class ParserDropWarehouseQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP WAREHOUSE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
