#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Query ADVISE db
*/
class ParserAdviseQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "Advise query simple specifier"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

}
