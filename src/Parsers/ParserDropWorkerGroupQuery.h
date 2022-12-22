#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{
class ParserDropWorkerGroupQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE WORKER GROUP query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
