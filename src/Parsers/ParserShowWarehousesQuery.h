#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses queries like
 * SHOW WAREHOUSES [where name LIKE 'pattern']
 */
class ParserShowWarehousesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW WAREHOUSES query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
