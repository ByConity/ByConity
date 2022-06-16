#pragma once

#include <Core/SettingsEnums.h>
#include <Parsers/IParser.h>


namespace DB
{

/** Base class for most parsers
  */
class IParserBase : public IParser
{
public:
    template <typename F>
    static bool wrapParseImpl(Pos & pos, const F & func)
    {
        Pos begin = pos;
        bool res = func();
        if (!res)
          pos = begin;
        return res;
    }

    struct IncreaseDepthTag {};

    template <typename F>
    static bool wrapParseImpl(Pos & pos, IncreaseDepthTag, const F & func)
    {
        Pos begin = pos;
        pos.increaseDepth();
        bool res = func();
        pos.decreaseDepth();
        if (!res)
          pos = begin;
        return res;
    }

    bool parse(Pos & pos, ASTPtr & node, Expected & expected) override;  // -V1071

protected:
    virtual bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) = 0;
};

struct ParserSettingsImpl
{
    bool parse_literal_as_decimal;
};

struct ParserSettings
{
    const static inline ParserSettingsImpl CLICKHOUSE {
        .parse_literal_as_decimal = false,
    };

    const static inline ParserSettingsImpl ANSI {
        // turn off decimal parsing to make sure TPC-DS can run
        // TODO: turn on if we have support implicit type conversion
        .parse_literal_as_decimal = false,
    };

    static ParserSettingsImpl valueOf(enum DialectType dt)
    {
        switch (dt)
        {
            case DialectType::CLICKHOUSE:
                return CLICKHOUSE;
            case DialectType::ANSI:
                return ANSI;
        }
    }
};

class IParserDialectBase : public IParserBase
{
public:
    explicit IParserDialectBase(ParserSettingsImpl t = ParserSettings::CLICKHOUSE) : dt(t) {}
protected:
    ParserSettingsImpl dt;
};

}
