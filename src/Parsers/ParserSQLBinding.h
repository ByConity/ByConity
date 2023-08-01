#pragma once
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/IParserBase.h>

namespace DB
{
/**
 * Create SQLBinding
 */
class ParserCreateBinding : public IParserDialectBase
{
public:
    explicit ParserCreateBinding(const ParserSettingsImpl & parser_settings_impl_ = ParserSettings::CLICKHOUSE)
        : IParserDialectBase(parser_settings_impl_)
    {
    }

protected:
    const char * getName() const override { return "Create SQLBinding"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/**
 * show Bindings;
 */
class ParserShowBindings : public IParserBase
{
protected:
    const char * getName() const override { return "Show Bindings"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/**
 * Drop Bindings For
 */
class ParserDropBinding : public IParserDialectBase
{
public:
    explicit ParserDropBinding(const ParserSettingsImpl & parser_settings_impl_ = ParserSettings::CLICKHOUSE)
        : IParserDialectBase(parser_settings_impl_)
    {
    }

protected:
    const char * getName() const override { return "Drop Binding"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
