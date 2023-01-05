#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{


class ParserExplainQuery : public IParserDialectBase
{
protected:
    const char * end;

    const char * getName() const override { return "EXPLAIN"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    ParserExplainQuery(const char* end_, ParserSettingsImpl t) : IParserDialectBase(t), end(end_) {}
};

}
