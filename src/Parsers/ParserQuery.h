#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserQuery : public IParserDialectBase
{
private:
    const char * end;

    const char * getName() const override { return "Query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserQuery(const char * end_, ParserSettingsImpl t = ParserSettings::CLICKHOUSE) : IParserDialectBase(t), end(end_) {}
};

}
