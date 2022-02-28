#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserSelectQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

}
