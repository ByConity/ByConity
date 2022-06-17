#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{
class ParserCreateWorkerGroupQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE WORKER GROUP query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
