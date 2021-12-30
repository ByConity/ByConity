#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDataType.h>
#include <Poco/String.h>


namespace DB
{

/** A nested table. For example, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
  */
class ParserNestedTable : public IParserBase
{
protected:
    const char * getName() const override { return "nested table"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Storage engine or Codec. For example:
 *         Memory()
 *         ReplicatedMergeTree('/path', 'replica')
 * Result of parsing - ASTFunction with or without parameters.
 */
class ParserIdentifierWithParameters : public IParserBase
{
protected:
    const char * getName() const override { return "identifier with parameters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

template <typename NameParser>
class IParserNameTypePair : public IParserBase
{
protected:
    const char * getName() const  override{ return "name and type pair"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** The name and type are separated by a space. For example, URL String. */
using ParserNameTypePair = IParserNameTypePair<ParserIdentifier>;

template <typename NameParser>
bool IParserNameTypePair<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    NameParser name_parser;
    ParserDataType type_parser;

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
class ParserNameTypePairList : public IParserBase
{
protected:
    const char * getName() const override { return "name and type pair list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** List of table names. */
class ParserNameList : public IParserBase
{
protected:
    const char * getName() const override { return "name list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


template <typename NameParser>
class IParserColumnDeclaration : public IParserBase
{
public:
    explicit IParserColumnDeclaration(bool require_type_ = true, bool allow_null_modifiers_ = false, bool check_keywords_after_name_ = false)
    : require_type(require_type_)
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
    ParserDataType type_parser;
    ParserKeyword s_default{"DEFAULT"};
    ParserKeyword s_null{"NULL"};
    ParserKeyword s_not{"NOT"};
    ParserKeyword s_materialized{"MATERIALIZED"};
    ParserKeyword s_alias{"ALIAS"};
    ParserKeyword s_comment{"COMMENT"};
    ParserKeyword s_codec{"CODEC"};
    ParserKeyword s_ttl{"TTL"};
    ParserKeyword s_remove{"REMOVE"};
    ParserKeyword s_kv{"KV"};
    ParserKeyword s_bitengine_encode{"BitEngineEncode"};
    ParserTernaryOperatorExpression expr_parser;
    ParserStringLiteral string_literal_parser;
    ParserCodec codec_parser;
    ParserExpression expression_parser;

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
    ASTPtr default_expression;
    ASTPtr comment_expression;
    ASTPtr codec_expression;
    ASTPtr ttl_expression;

    if (!s_default.checkWithoutMoving(pos, expected)
        && !s_materialized.checkWithoutMoving(pos, expected)
        && !s_alias.checkWithoutMoving(pos, expected)
        && (require_type
            || (!s_comment.checkWithoutMoving(pos, expected)
                && !s_codec.checkWithoutMoving(pos, expected))))
    {
        if (!type_parser.parse(pos, type, expected))
            return false;
    }

    Pos pos_before_specifier = pos;
    if (s_default.ignore(pos, expected) || s_materialized.ignore(pos, expected) || s_alias.ignore(pos, expected))
    {
        default_specifier = Poco::toUpper(std::string{pos_before_specifier->begin, pos_before_specifier->end});

        /// should be followed by an expression
        if (!expr_parser.parse(pos, default_expression, expected))
            return false;
    }

    if (require_type && !type && !default_expression)
        return false; /// reject column name without type

    if (type && allow_null_modifiers)
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

    if (s_comment.ignore(pos, expected))
    {
        /// should be followed by a string literal
        if (!string_literal_parser.parse(pos, comment_expression, expected))
            return false;
    }


    // Parse {COMPRESSION, SECURITY [ENCRYPT], BLOOM, KV, BitmapIndex}
    // bool compression = false;
    // bool security = false;
    // bool encrypt = false;
    // bool bloom = false;
    bool kv = false;
    bool bitengine_encode = false;
    // bool bitmap_index = false;
    // bool mark_bitmap_index = false;
    // bool inner_compression =false;
    // bool inner_security = false;
    // bool inner_encrypt = false;
    // bool inner_bloom = false;
    bool inner_kv = false;
    bool inner_bitengine_encode = false;
    // bool inner_bitmap_index = false;
    // bool inner_mark_bitmap_index = false;
    do {
    //     inner_compression = s_compression.ignore(pos, expected);
    //     inner_security = s_security.ignore(pos, expected);
    //     inner_encrypt = s_encrypt.ignore(pos, expected);
    //     inner_bloom = s_bloom.ignore(pos, expected);
        inner_kv = s_kv.ignore(pos, expected);
        inner_bitengine_encode = s_bitengine_encode.ignore(pos, expected);
        // inner_bitmap_index = s_bitmap_index.ignore(pos, expected);
        // inner_mark_bitmap_index = s_mark_bitmap_index.ignore(pos, expected);
        // // each setting can only appear once
        // if (compression && inner_compression)
        //     return false;
        // if (security && inner_security)
        //     return false;
        // if (inner_encrypt && encrypt)
        //     return false;
        // if (bloom && inner_bloom)
        //     return false;
        if (kv && inner_kv)
            return false;
        if (bitengine_encode && inner_bitengine_encode)
            return false;
    //     if (bitmap_index && inner_bitmap_index)
    //         return false;
    //     if (mark_bitmap_index && inner_mark_bitmap_index)
    //         return false;
    //     compression |= inner_compression;
    //     security |= inner_security;
    //     encrypt |= inner_encrypt;
    //     bloom |= inner_bloom;
        kv |= inner_kv;
        bitengine_encode |= inner_bitengine_encode;
    //     bitmap_index |= inner_bitmap_index;
    //     mark_bitmap_index |= inner_mark_bitmap_index;
    } while (/*inner_compression || inner_security || inner_encrypt || inner_bloom ||*/ inner_kv || inner_bitengine_encode /*|| inner_bitmap_index*/);


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

    if (default_expression)
    {
        column_declaration->default_specifier = default_specifier;
        column_declaration->default_expression = default_expression;
        column_declaration->children.push_back(std::move(default_expression));
    }

    // if (bloom) column_declaration->flags |= TYPE_BLOOM_FLAG;

    // if (bitmap_index) column_declaration->flags |= TYPE_BITMAP_INDEX_FLAG;

    // if (mark_bitmap_index) column_declaration->flags |= TYPE_MARK_BITMAP_INDEX_FALG;

    // if (compression) column_declaration->flags |= TYPE_COMPRESSION_FLAG;

    // if (security) column_declaration->flags |= TYPE_SECURITY_FLAG;

    // if (encrypt) column_declaration->flags |= TYPE_ENCRYPT_FLAG;

    if (kv) column_declaration->flags |= TYPE_MAP_KV_STORE_FLAG;

    if (bitengine_encode) column_declaration->flags |= TYPE_BITENGINE_ENCODE_FLAG;

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

class ParserColumnDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "column declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** name BY expr TYPE typename(arg1, arg2, ...) GRANULARITY value */
class ParserIndexDeclaration : public IParserBase
{
public:
    ParserIndexDeclaration() {}

protected:
    const char * getName() const override { return "index declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserConstraintDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "constraint declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserProjectionDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "projection declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserTablePropertyDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "table property (column, index, constraint) declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserIndexDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "index declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserConstraintDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "constraint declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserProjectionDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "projection declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserTablePropertiesDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "columns or indices declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/**
  * ENGINE = name [PARTITION BY expr] [ORDER BY expr] [PRIMARY KEY expr] [UNIQUE KEY expr] [SAMPLE BY expr] [SETTINGS name = value, ...]
  */
class ParserStorage : public IParserBase
{
protected:
    const char * getName() const override { return "storage definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
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
class ParserCreateTableQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE|ATTACH LIVE VIEW [IF NOT EXISTS] [db.]name [UUID 'uuid'] [TO [db.]name] AS SELECT ...
class ParserCreateLiveViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE LIVE VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE|ATTACH DATABASE db [ENGINE = engine]
class ParserCreateDatabaseQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE DATABASE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE[OR REPLACE]|ATTACH [[MATERIALIZED] VIEW] | [VIEW]] [IF NOT EXISTS] [db.]name [UUID 'uuid'] [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
class ParserCreateViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Parses complete dictionary create query. Uses ParserDictionary and
/// ParserDictionaryAttributeDeclaration. Produces ASTCreateQuery.
/// CREATE DICTIONARY [IF NOT EXISTS] [db.]name (attrs) PRIMARY KEY key SOURCE(s(params)) LAYOUT(l(params)) LIFETIME([min v1 max] v2) [RANGE(min v1 max v2)]
class ParserCreateDictionaryQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE DICTIONARY"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
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
class ParserCreateQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
