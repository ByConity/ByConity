#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserProjectionSelectQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "PROJECTION SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

}
