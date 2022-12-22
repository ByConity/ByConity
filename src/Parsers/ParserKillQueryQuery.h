#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** KILL QUERY WHERE <logical expression upon system.processes fields> [SYNC|ASYNC|TEST]
  */
class ParserKillQueryQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "KILL QUERY query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

}

