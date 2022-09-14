#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>

namespace DB
{


class ParserPartToolkitQuery : public IParserDialectBase
{
protected:
    const char * end;
    const char * getName() const override { return "Part toolkit"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    explicit ParserPartToolkitQuery(const char * end_, ParserSettingsImpl t = ParserSettings::CLICKHOUSE) : IParserDialectBase(t), end(end_) {}
};


/**
  * Almost the same with ParserStorage, but without ENGINE type.
  * [PARTITION BY expr] [ORDER BY expr] [PRIMARY KEY expr] [UNIQUE KEY expr] [SAMPLE BY expr] [SETTINGS name = value, ...]
  */
class ParserPWStorage : public IParserDialectBase
{
protected:
    const char * getName() const override { return "PartToolkit storage definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

}
