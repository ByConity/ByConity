#pragma once
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/IParserBase.h>


namespace DB
{

class ParserPreparedParameter : public IParserDialectBase
{
protected:
    const char * getName() const override
    {
        return "PreparedParameter";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

}
