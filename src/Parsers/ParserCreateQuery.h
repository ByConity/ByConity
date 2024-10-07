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

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDataType.h>
#include <Poco/String.h>
#include "Parsers/IAST_fwd.h"


namespace DB
{

/** A nested table. For example, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
  */
class ParserNestedTable : public IParserDialectBase
{
protected:
    const char * getName() const override { return "nested table"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};


/** Storage engine or Codec. For example:
 *         Memory()
 *         ReplicatedMergeTree('/path', 'replica')
 * Result of parsing - ASTFunction with or without parameters.
 */
class ParserIdentifierWithParameters : public IParserDialectBase
{
protected:
    const char * getName() const override { return "identifier with parameters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

template <typename NameParser>
class IParserNameTypePair : public IParserDialectBase
{
protected:
    const char * getName() const  override{ return "name and type pair"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

/** The name and type are separated by a space. For example, URL String. */
using ParserNameTypePair = IParserNameTypePair<ParserIdentifier>;

template <typename NameParser>
bool IParserNameTypePair<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    NameParser name_parser;
    ParserDataType type_parser(dt, false);

    ASTPtr name, type;
    if (name_parser.parse(pos, name, expected)
        && type_parser.parse(pos, type, expected))
    {
        auto name_type_pair = std::make_shared<ASTNameTypePair>();
        tryGetIdentifierNameInto(name, name_type_pair->name);
        name_type_pair->type = type;
        name_type_pair->children.push_back(type);
        node = name_type_pair;
        return true;
    }

    return false;
}

/** List of columns. */
class ParserNameTypePairList : public IParserDialectBase
{
protected:
    const char * getName() const override { return "name and type pair list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

/** List of table names. */
class ParserNameList : public IParserDialectBase
{
protected:
    const char * getName() const override { return "name list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};


template <typename NameParser>
class IParserColumnDeclaration : public IParserDialectBase
{
public:
    explicit IParserColumnDeclaration(ParserSettingsImpl t, bool require_type_ = true, bool allow_null_modifiers_ = false, bool check_keywords_after_name_ = false)
    : IParserDialectBase(t)
    , require_type(require_type_)
    , allow_null_modifiers(allow_null_modifiers_)
    , check_keywords_after_name(check_keywords_after_name_)
    {
    }

protected:
    using ASTDeclarePtr = std::shared_ptr<ASTColumnDeclaration>;

    const char * getName() const  override{ return "column declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool require_type = true;
    bool allow_null_modifiers = false;
    bool check_keywords_after_name = false;
};

using ParserColumnDeclaration = IParserColumnDeclaration<ParserIdentifier>;
using ParserCompoundColumnDeclaration = IParserColumnDeclaration<ParserCompoundIdentifier>;

template <typename NameParser>
bool IParserColumnDeclaration<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    NameParser name_parser;
    ParserDataType type_parser(dt);
    ParserKeyword s_default{"DEFAULT"};
    ParserKeyword s_auto_increment{"AUTO_INCREMENT"};
    ParserKeyword s_null{"NULL"};
    ParserKeyword s_not{"NOT"};
    ParserKeyword s_pk{"PRIMARY KEY"};
    ParserKeyword s_materialized{"MATERIALIZED"};
    ParserKeyword s_alias{"ALIAS"};
    ParserKeyword s_comment{"COMMENT"};
    ParserKeyword s_codec{"CODEC"};
    ParserKeyword s_ttl{"TTL"};
    ParserKeyword s_remove{"REMOVE"};
    ParserKeyword s_compression{"COMPRESSION"};
    ParserKeyword s_bloom{"BLOOM"};
    ParserKeyword s_bitmap_index{"BitmapIndex"};
    ParserKeyword s_security{"SECURITY"}; /// just for compatible
    ParserKeyword s_encrypt{"ENCRYPT"};   /// just for compatible
    // ParserKeyword s_segment_bitmap_index{"SegmentBitmapIndex"};
    ParserKeyword s_kv{"KV"};
    ParserKeyword s_byte{"BYTE"};
    ParserKeyword s_bitengine_encode{"BitEngineEncode"};
    ParserTernaryOperatorExpression expr_parser(dt); /* decimal type can use float as default value */
    ParserStringLiteral string_literal_parser;
    ParserCodec codec_parser;
    ParserExpression expression_parser(ParserSettings::CLICKHOUSE); /* Use CK dialect to parse TTL */

    /// Dummy MySQL keywords
    ParserKeyword s_on_update("ON UPDATE");
    ParserKeyword s_charset1("CHARSET");
    ParserKeyword s_default_charset1("DEFAULT CHARSET");
    ParserKeyword s_charset2("CHARACTER SET");
    ParserKeyword s_default_charset2("DEFAULT CHARACTER SET");
    ParserKeyword s_collate("COLLATE");
    ParserKeyword s_default_collate("DEFAULT COLLATE");
    ParserKeyword s_signed("SIGNED");
    ParserKeyword s_unsigned("UNSIGNED");
    ParserKeyword s_zerofill("ZEROFILL");

    /// mandatory column name
    ASTPtr name;
    if (!name_parser.parse(pos, name, expected))
        return false;

    const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
    tryGetIdentifierNameInto(name, column_declaration->name);

    /// This keyword may occur only in MODIFY COLUMN query. We check it here
    /// because ParserDataType parses types as an arbitrary identifiers and
    /// doesn't check that parsed string is existing data type. In this way
    /// REMOVE keyword can be parsed as data type and further parsing will fail.
    /// So we just check this keyword and in case of success return column
    /// declaration with name only.
    if (!require_type && s_remove.checkWithoutMoving(pos, expected))
    {
        if (!check_keywords_after_name)
            return false;

        node = column_declaration;
        return true;
    }

    /** column name should be followed by type name if it
      *    is not immediately followed by {DEFAULT, MATERIALIZED, ALIAS, COMMENT}
      */
    ASTPtr type;
    String default_specifier;
    std::optional<bool> null_modifier;
    std::optional<bool> unsigned_modifier;
    ASTPtr default_expression;
    ASTPtr comment_expression;
    ASTPtr codec_expression;
    ASTPtr ttl_expression;
    ASTPtr charset_expression;
    ASTPtr collate_expression;
    ASTPtr on_update_expression;

    auto null_check_without_moving = [&]() -> bool
    {
        if (!allow_null_modifiers)
            return false;

        if (s_null.checkWithoutMoving(pos, expected))
            return true;

        Pos before_null = pos;
        bool res = s_not.check(pos, expected) && s_null.checkWithoutMoving(pos, expected);
        pos = before_null;
        return res;
    };

    if (!null_check_without_moving()
        && !s_pk.checkWithoutMoving(pos, expected)
        && !s_default.checkWithoutMoving(pos, expected)
        && !s_auto_increment.checkWithoutMoving(pos, expected)
        && !s_materialized.checkWithoutMoving(pos, expected)
        && !s_alias.checkWithoutMoving(pos, expected)
        && (require_type
            || (!s_comment.checkWithoutMoving(pos, expected)
                && !s_codec.checkWithoutMoving(pos, expected))))
    {
        if (!type_parser.parse(pos, type, expected))
            return false;
        if (s_signed.ignore(pos, expected))
            unsigned_modifier = false;
        if (s_unsigned.ignore(pos, expected))
            unsigned_modifier = true;
    }

    s_zerofill.ignore(pos, expected);

    if (s_charset1.ignore(pos, expected) || s_default_charset1.ignore(pos, expected) || s_charset2.ignore(pos, expected)
        || s_default_charset2.ignore(pos, expected))
    {
        if (!expression_parser.parse(pos, charset_expression, expected))
            return false;
    }

    if (s_collate.ignore(pos, expected) || s_default_collate.ignore(pos, expected))
    {
        if (!expression_parser.parse(pos, charset_expression, expected))
            return false;
    }

    if (allow_null_modifiers)
    {
        if (s_not.check(pos, expected))
        {
            if (!s_null.check(pos, expected))
                return false;
            null_modifier.emplace(false);
        }
        else if (s_null.check(pos, expected))
            null_modifier.emplace(true);
    }

    Pos pos_before_specifier = pos;
    if (s_default.ignore(pos, expected) || s_materialized.ignore(pos, expected) || s_alias.ignore(pos, expected))
    {
        default_specifier = Poco::toUpper(std::string{pos_before_specifier->begin, pos_before_specifier->end});

        /// should be followed by an expression
        if (!expr_parser.parse(pos, default_expression, expected))
            return false;
    }
    else if (s_auto_increment.ignore(pos, expected)) {
        column_declaration->auto_increment = true;
    }

    if (require_type && !type && !default_expression)
        return false; /// reject column name without type

    if ((type || default_expression) && allow_null_modifiers && !null_modifier.has_value())
    {
        if (s_not.ignore(pos, expected))
        {
            if (!s_null.ignore(pos, expected))
                return false;
            null_modifier.emplace(false);
        }
        else if (s_null.ignore(pos, expected))
            null_modifier.emplace(true);
    }

    if (s_on_update.ignore(pos, expected))
    {
        if (!expression_parser.parse(pos, on_update_expression, expected))
            return false;
    }

    if (s_pk.ignore(pos, expected))
    {
        column_declaration->mysql_primary_key = true;
    }

    if (s_comment.ignore(pos, expected))
    {
        /// should be followed by a string literal
        if (!string_literal_parser.parse(pos, comment_expression, expected))
            return false;
    }

    UInt16 flags = 0;
    while (true)
    {
        UInt16 inner_flags = 0;

        if (s_kv.ignore(pos, expected))
            inner_flags |= TYPE_MAP_KV_STORE_FLAG;
        if (s_compression.ignore(pos, expected))
            inner_flags |= TYPE_COMPRESSION_FLAG;
        if (s_bitengine_encode.ignore(pos, expected))
            inner_flags |= TYPE_BITENGINE_ENCODE_FLAG;
        if (s_byte.ignore(pos, expected))
            inner_flags |= TYPE_MAP_BYTE_STORE_FLAG;
        if (s_bloom.ignore(pos, expected))
            inner_flags |= TYPE_BLOOM_FLAG;
        if (s_bitmap_index.ignore(pos, expected))
            inner_flags |= TYPE_BITMAP_INDEX_FLAG;
        // if (s_segment_bitmap_index.ignore(pos, expected))
        //     inner_flags |= TYPE_SEGMENT_BITMAP_INDEX_FLAG;
        if (s_security.ignore(pos, expected))
            inner_flags |= TYPE_SECURITY_FLAG;
        if (s_encrypt.ignore(pos, expected))
            inner_flags |= TYPE_ENCRYPT_FLAG;

        if (!inner_flags)
            break;

        if (flags & inner_flags)
            return false;

        flags |= inner_flags;

        /// map kv flag and map byte flag cannot be set at the same time
        if ((flags & TYPE_MAP_KV_STORE_FLAG) && (flags & TYPE_MAP_BYTE_STORE_FLAG))
            return false;
    }

    column_declaration->flags = flags;

    if (s_codec.ignore(pos, expected))
    {
        if (!codec_parser.parse(pos, codec_expression, expected))
            return false;
    }
    if (s_ttl.ignore(pos, expected))
    {
        if (!expression_parser.parse(pos, ttl_expression, expected))
            return false;
    }

    node = column_declaration;

    if (type)
    {
        column_declaration->type = type;
        column_declaration->children.push_back(std::move(type));
    }
    column_declaration->null_modifier = null_modifier;
    column_declaration->unsigned_modifier = unsigned_modifier;
    if (default_expression)
    {
        column_declaration->default_specifier = default_specifier;
        column_declaration->default_expression = default_expression;
        column_declaration->children.push_back(std::move(default_expression));
    }

    if (on_update_expression)
    {
        column_declaration->on_update_expression = on_update_expression;
        column_declaration->children.push_back(std::move(on_update_expression));
    }

    if (comment_expression)
    {
        column_declaration->comment = comment_expression;
        column_declaration->children.push_back(std::move(comment_expression));
    }

    if (codec_expression)
    {
        column_declaration->codec = codec_expression;
        column_declaration->children.push_back(std::move(codec_expression));
    }

    if (ttl_expression)
    {
        column_declaration->ttl = ttl_expression;
        column_declaration->children.push_back(std::move(ttl_expression));
    }

    return true;
}

class ParserColumnDeclarationList : public IParserDialectBase
{
protected:
    const char * getName() const override { return "column declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};


/** name BY expr TYPE typename(arg1, arg2, ...) GRANULARITY value */
class ParserIndexDeclaration : public IParserDialectBase
{
protected:
    const char * getName() const override { return "index declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserConstraintDeclaration : public IParserDialectBase
{
protected:
    const char * getName() const override { return "constraint declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserBitEngineConstraintDeclaration : public IParserDialectBase
{
protected:
    const char * getName() const override { return "bitengine constraint declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserForeignKeyDeclaration : public IParserDialectBase
{
protected:
    const char * getName() const override
    {
        return "foreign key declaration";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserUniqueNotEnforcedDeclaration : public IParserDialectBase
{
protected:
    const char * getName() const override
    {
        return "unique not enforced declaration";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserProjectionDeclaration : public IParserDialectBase
{
protected:
    const char * getName() const override { return "projection declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserTablePropertyDeclaration : public IParserDialectBase
{
protected:
    const char * getName() const override { return "table property (column, index, constraint) declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};


class ParserIndexDeclarationList : public IParserDialectBase
{
protected:
    const char * getName() const override { return "index declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserConstraintDeclarationList : public IParserDialectBase
{
protected:
    const char * getName() const override { return "constraint declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserBitEngineConstraintDeclarationList : public IParserDialectBase
{
protected:
    const char * getName() const override { return "bitengine constraint declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserForeignKeyDeclarationList : public IParserDialectBase
{
protected:
    const char * getName() const override
    {
        return "foreign key declaration list";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserUniqueNotEnforcedDeclarationList : public IParserDialectBase
{
protected:
    const char * getName() const override
    {
        return "unique not enforced declaration list";
    }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserProjectionDeclarationList : public IParserDialectBase
{
protected:
    const char * getName() const override { return "projection declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};


class ParserTablePropertiesDeclarationList : public IParserDialectBase
{
protected:
    const char * getName() const override { return "columns or indices declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};


/**
  * ENGINE = name [PARTITION BY expr] [ORDER BY expr] [PRIMARY KEY expr] [UNIQUE KEY expr] [SAMPLE BY expr] [SETTINGS name = value, ...]
  */
class ParserStorage : public IParserDialectBase
{
public:
    using IParserDialectBase::IParserDialectBase;

    /// What kind of engine we're going to parse.
    enum EngineKind
    {
        TABLE_ENGINE,
        DATABASE_ENGINE,
    };

    ParserStorage(EngineKind engine_kind_, ParserSettingsImpl t = ParserSettings::CLICKHOUSE)
        : IParserDialectBase(t), engine_kind(engine_kind_) {}
protected:
    const char * getName() const override { return "storage definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    EngineKind engine_kind;
};

/** Query like this:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster]
  * (
  *     name1 type1,
  *     name2 type2,
  *     ...
  *     INDEX name1 expr TYPE type1(args) GRANULARITY value,
  *     ...
  * ) ENGINE = engine
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster] AS [db2.]name2 [ENGINE = engine]
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster] AS ENGINE = engine SELECT ...
  *
  */
class ParserCreateTableQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

/**
  * table_attribute [partition_options] [storage_policy] [block_size] [engine] [rt_engine] [table_properties]
  */
class ParserStorageMySQL : public IParserDialectBase
{
protected:
    const char * getName() const override { return "storage definition for MySQL"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

/**
  * Please refer to the doc of supported syntax
  */
class ParserCreateTableAnalyticalMySQLQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE TABLE query for MySQL"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

/// CREATE|ATTACH LIVE VIEW [IF NOT EXISTS] [db.]name [UUID 'uuid'] [TO [db.]name] AS SELECT ...
class ParserCreateLiveViewQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE LIVE VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserTableOverrideDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "table override declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserTableOverridesDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "table overrides declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE|ATTACH DATABASE db [ENGINE = engine]
class ParserCreateDatabaseQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE DATABASE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

class ParserCreateCatalogQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE EXTERNAL CATALOG query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

/// CREATE[OR REPLACE]|ATTACH [[MATERIALIZED] VIEW] | [VIEW]] [IF NOT EXISTS] [db.]name [UUID 'uuid'] [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
class ParserCreateViewQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

/// Parses complete dictionary create query. Uses ParserDictionary and
/// ParserDictionaryAttributeDeclaration. Produces ASTCreateQuery.
/// CREATE DICTIONARY [IF NOT EXISTS] [db.]name (attrs) PRIMARY KEY key SOURCE(s(params)) LAYOUT(l(params)) LIFETIME([min v1 max] v2) [RANGE(min v1 max v2)]
class ParserCreateDictionaryQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE DICTIONARY"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

/// Parses queries like:
/// CREATE SNAPSHOT [IF NOT EXISTS] [db.]name [TO [db.]table] TTL n DAYS;
class ParserCreateSnapshotQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE SNAPSHOT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    using IParserDialectBase::IParserDialectBase;
};

/** Query like this:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name
  * (
  *     name1 type1,
  *     name2 type2,
  *     ...
  *     INDEX name1 expr TYPE type1(args) GRANULARITY value,
  *     ...
  *     PRIMARY KEY expr
  * ) ENGINE = engine
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name AS [db2.]name2 [ENGINE = engine]
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name AS ENGINE = engine SELECT ...
  *
  * Or:
  * CREATE|ATTACH DATABASE db [ENGINE = engine]
  *
  * Or:
  * CREATE[OR REPLACE]|ATTACH [[MATERIALIZED] VIEW] | [VIEW]] [IF NOT EXISTS] [db.]name [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
  */
class ParserCreateQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

}
