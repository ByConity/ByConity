#pragma once
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/IParserBase.h>

namespace DB
{
/**
 * Create Prepared Statement
 */
class ParserCreatePreparedStatementQuery : public IParserDialectBase
{
public:
    explicit ParserCreatePreparedStatementQuery(const ParserSettingsImpl & parser_settings_impl_ = ParserSettings::CLICKHOUSE)
        : IParserDialectBase(parser_settings_impl_)
    {
    }

protected:
    const char * getName() const override
    {
        return "CreatePreparedStatement";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/**
 * Execute Prepared Statement
 */
class ParserExecutePreparedStatementQuery : public IParserBase
{
protected:
    const char * getName() const override
    {
        return "ExecutePreparedStatement";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/**
 * Show Prepared Statement
 */
class ParserShowPreparedStatementQuery : public IParserBase
{
protected:
    const char * getName() const override
    {
        return "ShowPreparedStatement";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/**
 * Drop Prepared Statement
 */
class ParserDropPreparedStatementQuery : public IParserBase
{
protected:
    const char * getName() const override
    {
        return "DropPreparedStatement";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
