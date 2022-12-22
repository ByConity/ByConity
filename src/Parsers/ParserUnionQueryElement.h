#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserUnionQueryElement : public IParserDialectBase
{
protected:
    const char * getName() const override { return "SELECT query, subquery, possibly with UNION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

}
