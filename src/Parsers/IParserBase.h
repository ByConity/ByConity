/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

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
    mutable bool parse_literal_as_decimal;

    /// determine if apply the rewritings for adaptive type cast
    mutable bool apply_adaptive_type_cast;

    /// update mutable items with the settings of current context
    void changeMutableSettings(const Settings & s) const
    {
        apply_adaptive_type_cast = s.adaptive_type_cast;
        parse_literal_as_decimal = s.parse_literal_as_decimal;
    }

    /// update mutable items with other ParserSettingsImpl
    void changeMutableSettings(const ParserSettingsImpl & s) const
    {
        apply_adaptive_type_cast = s.apply_adaptive_type_cast;
        parse_literal_as_decimal = s.parse_literal_as_decimal;
    }

    /// demonstrate nullable info with explicit null modifiers (including nested types)
    bool explicit_null_modifiers;
    bool parse_mysql_ddl;
    bool parse_bitwise_operators;
    /// treat " as identifier quote character (like the ` quote character) and not as a string quote character
    bool ansi_quotes;
};

struct ParserSettings
{
    const static inline ParserSettingsImpl CLICKHOUSE{
        .parse_literal_as_decimal = false,
        .apply_adaptive_type_cast = false,
        .explicit_null_modifiers = false,
        .parse_mysql_ddl = false,
        .parse_bitwise_operators = false,
        .ansi_quotes = true,
    };

    const static inline ParserSettingsImpl MYSQL{
        .parse_literal_as_decimal = true,
        .apply_adaptive_type_cast = false,
        .explicit_null_modifiers = true,
        .parse_mysql_ddl = true,
        .parse_bitwise_operators = true,
        .ansi_quotes = false,
    };

    const static inline ParserSettingsImpl ANSI{
        .parse_literal_as_decimal = true,
        .apply_adaptive_type_cast = false,
        .explicit_null_modifiers = true,
        .parse_mysql_ddl = false,
        .parse_bitwise_operators = false,
        .ansi_quotes = true,
    };

    // deprecated. use `valueOf(const Settings & s)` instead
    static ParserSettingsImpl valueOf(enum DialectType dt)
    {
        switch (dt)
        {
            case DialectType::CLICKHOUSE:
                return CLICKHOUSE;
            case DialectType::ANSI:
                return ANSI;
            case DialectType::MYSQL:
                return MYSQL;
        }
    }

    static ParserSettingsImpl valueOf(const Settings & s)
    {
        const auto setting_impl = [&]() -> ParserSettingsImpl {
            switch (s.dialect_type) {
                case DialectType::CLICKHOUSE: return CLICKHOUSE;
                case DialectType::ANSI: return ANSI;
                case DialectType::MYSQL: return MYSQL;
                default:
                    throw std::invalid_argument("Unsupported DialectType");
            }
        }();
        setting_impl.changeMutableSettings(s);
        return setting_impl;
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
