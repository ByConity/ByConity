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

#include <memory>
#include <Common/typeid_cast.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateQueryAnalyticalMySQL.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUniqueNotEnforcedDeclaration.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTTableOverrides.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTBitEngineConstraintDeclaration.h>
#include <Parsers/queryToString.h>
#include <Parsers/ParserDictionary.h>
#include <Parsers/ParserDictionaryAttributeDeclaration.h>
#include <Parsers/ParserProjectionSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserRefreshStrategy.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Poco/Logger.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

ASTPtr parseComment(IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_comment("COMMENT");
    ParserToken s_eq(TokenType::Equals);
    ParserStringLiteral string_literal_parser;
    ASTPtr comment;
    s_comment.ignore(pos, expected);
    s_eq.ignore(pos, expected);
    string_literal_parser.parse(pos, comment, expected);

    return comment;
}

}

bool ParserNestedTable::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p;
    ParserNameTypePairList columns_p(dt);

    ASTPtr name;
    ASTPtr columns;

    /// For now `name == 'Nested'`, probably alternative nested data structures will appear
    if (!name_p.parse(pos, name, expected))
        return false;

    if (!open.ignore(pos))
        return false;

    if (!columns_p.parse(pos, columns, expected))
        return false;

    if (!close.ignore(pos))
        return false;

    auto func = std::make_shared<ASTFunction>();
    tryGetIdentifierNameInto(name, func->name);
    // FIXME(ilezhankin): func->no_empty_args = true; ?
    func->arguments = columns;
    func->children.push_back(columns);
    node = func;

    return true;
}


bool ParserIdentifierWithParameters::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserFunction(dt).parse(pos, node, expected);
}

bool ParserNameTypePairList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserNameTypePair>(dt), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserColumnDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserColumnDeclaration>(dt), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserNameList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserCompoundIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserIndexDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_type("TYPE");
    ParserKeyword s_granularity("GRANULARITY");

    ParserIdentifier name_p;
    ParserDataType data_type_p(dt);
    ParserExpression expression_p(dt);
    ParserUnsignedInteger granularity_p;

    ASTPtr name;
    ASTPtr expr;
    ASTPtr type;
    ASTPtr granularity;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!expression_p.parse(pos, expr, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (!data_type_p.parse(pos, type, expected))
        return false;

    if (!s_granularity.ignore(pos, expected))
        return false;

    if (!granularity_p.parse(pos, granularity, expected))
        return false;

    auto index = std::make_shared<ASTIndexDeclaration>();
    index->name = name->as<ASTIdentifier &>().name();
    index->granularity = granularity->as<ASTLiteral &>().value.safeGet<UInt64>();
    index->set(index->expr, expr);
    index->set(index->type, type);
    node = index;

    return true;
}

String getDefaultName(ASTPtr column_names, const String & suffix)
{
    String name;
    if (auto *const col_name_list = column_names->as<ASTExpressionList>())
    {
        for (const auto &col_name: col_name_list->children)
        {
            name += col_name->as<ASTIdentifier &>().name() + "_";
        }
    }
    else
    {
        auto unix_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        std::uniform_int_distribution dist;
        name = std::to_string(unix_time) + "_" + std::to_string(dist(thread_local_rng)) + "_";
    }
    return name + suffix;
}

bool ParserConstraintDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check("CHECK");

    ParserIdentifier name_p;
    ParserLogicalOrExpression expression_p(dt);

    ASTPtr name;
    ASTPtr expr;

    if (!s_check.ignore(pos, expected))
    {
        if (!name_p.parse(pos, name, expected))
            return false;

        if (!s_check.ignore(pos, expected))
            return false;
    }

    if (!expression_p.parse(pos, expr, expected))
        return false;

    auto constraint = std::make_shared<ASTConstraintDeclaration>();
    if (name)
        constraint->name = name->as<ASTIdentifier &>().name();
    else
        constraint->name = getDefaultName(expr, "check");
    constraint->set(constraint->expr, expr);
    node = constraint;

    return true;
}

bool ParserBitEngineConstraintDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check("CHECK");

    ParserIdentifier name_p;
    ParserLogicalOrExpression expression_p(dt);

    ASTPtr name;
    ASTPtr expr;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_check.ignore(pos, expected))
        return false;

    if (!expression_p.parse(pos, expr, expected))
        return false;

    auto constraint = std::make_shared<ASTBitEngineConstraintDeclaration>();
    constraint->name = name->as<ASTIdentifier &>().name();
    constraint->set(constraint->expr, expr);
    node = constraint;

    return true;
}

bool ParserForeignKeyDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier name_p;
    ParserKeyword s_foreign_key("FOREIGN KEY");
    ParserKeyword s_references("REFERENCES");
    ParserKeyword s_match_full("MATCH FULL");
    ParserKeyword s_match_partial("MATCH PARTIAL");
    ParserKeyword s_on_update("ON UPDATE");
    ParserKeyword s_on_delete("ON DELETE");

    ParserKeyword s_restrict("RESTRICT");
    ParserKeyword s_cascade("CASCADE");
    ParserKeyword s_set_null("SET NULL");
    ParserKeyword s_no_action("NO ACTION");
    ParserKeyword s_set_default("SET DEFAULT");

    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    ParserList foreign_key_columns_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserIdentifier ref_table_name_p;
    ParserList references_columns_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    ASTPtr fk_name;
    ASTPtr column_names;
    ASTPtr ref_table_name;
    ASTPtr ref_column_names;

    if (!s_foreign_key.ignore(pos, expected))
    {
        if (!name_p.parse(pos, fk_name, expected))
            return false;
        if (!s_foreign_key.ignore(pos, expected))
            return false;
    }

    if (!fk_name)
        name_p.parse(pos, fk_name, expected);

    if (!s_lparen.ignore(pos, expected))
        return false;
    if (!foreign_key_columns_p.parse(pos, column_names, expected))
        return false;
    if (!s_rparen.ignore(pos, expected))
        return false;

    if (!s_references.ignore(pos, expected))
        return false;

    if (!ref_table_name_p.parse(pos, ref_table_name, expected))
        return false;

    if (!s_lparen.ignore(pos, expected))
        return false;
    if (!references_columns_p.parse(pos, ref_column_names, expected))
        return false;
    if (!s_rparen.ignore(pos, expected))
        return false;

    s_match_full.ignore(pos, expected);
    s_match_partial.ignore(pos, expected);

    if (s_on_update.ignore(pos, expected) || s_on_delete.ignore(pos, expected))
    {
        if (!s_restrict.ignore(pos, expected) && !s_cascade.ignore(pos, expected)
            && !s_set_null.ignore(pos, expected) && !s_no_action.ignore(pos, expected)
            && !s_set_default.ignore(pos, expected))
            return false;
    }

    if (s_on_update.ignore(pos, expected) || s_on_delete.ignore(pos, expected))
    {
        if (!s_restrict.ignore(pos, expected) && !s_cascade.ignore(pos, expected)
            && !s_set_null.ignore(pos, expected) && !s_no_action.ignore(pos, expected)
            && !s_set_default.ignore(pos, expected))
            return false;
    }

    auto foreign_key = std::make_shared<ASTForeignKeyDeclaration>();
    if (fk_name)
        foreign_key->fk_name = fk_name->as<ASTIdentifier &>().name();
    else
        foreign_key->fk_name = getDefaultName(column_names, "fk");
    foreign_key->column_names = column_names;
    foreign_key->ref_table_name = ref_table_name->as<ASTIdentifier &>().name();
    foreign_key->ref_column_names = ref_column_names;

    node = foreign_key;

    return true;
}

bool ParserUniqueNotEnforcedDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier name_p;
    ParserKeyword s_unique_not_enforced("UNIQUE");

    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    ParserList unique_columns_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    ASTPtr name;
    ASTPtr column_names;

    if (!s_unique_not_enforced.ignore(pos, expected))
    {
        if (!name_p.parse(pos, name, expected))
            return false;
        if (!s_unique_not_enforced.ignore(pos, expected))
            return false;
    }

    if (!name)
        name_p.parse(pos, name, expected);

    if (!s_lparen.ignore(pos, expected))
        return false;
    if (!unique_columns_p.parse(pos, column_names, expected))
        return false;
    if (!s_rparen.ignore(pos, expected))
        return false;


    auto unique = std::make_shared<ASTUniqueNotEnforcedDeclaration>();
    if (name)
        unique->name = name->as<ASTIdentifier &>().name();
    else
        unique->name = getDefaultName(column_names, "uk");
    unique->column_names = column_names;

    node = unique;

    return true;
}

bool ParserProjectionDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier name_p;
    ParserProjectionSelectQuery query_p(dt);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ASTPtr name;
    ASTPtr query;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_lparen.ignore(pos, expected))
        return false;

    if (!query_p.parse(pos, query, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    auto projection = std::make_shared<ASTProjectionDeclaration>();
    projection->name = name->as<ASTIdentifier &>().name();
    projection->set(projection->query, query);
    node = projection;

    return true;
}


bool ParserTablePropertyDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_index("INDEX");
    ParserKeyword s_key("KEY");
    ParserKeyword s_cluster_key("CLUSTERED KEY");
    ParserKeyword s_constraint("CONSTRAINT");
    ParserKeyword s_foreign_key("FOREIGN KEY");
    ParserKeyword s_unique("UNIQUE");

    ParserKeyword s_bitengine_constraint("BITENGINE_CONSTRAINT");
    ParserKeyword s_projection("PROJECTION");
    ParserKeyword s_primary_key("PRIMARY KEY");

    ParserIndexDeclaration index_p(dt);
    ParserConstraintDeclaration constraint_p(dt);
    ParserBitEngineConstraintDeclaration bitengine_constraint_p(dt);
    ParserProjectionDeclaration projection_p(dt);
    ParserColumnDeclaration column_p{dt, true, true};
    ParserExpression primary_key_p(dt);
    ParserForeignKeyDeclaration foreign_key_p(dt);
    ParserUniqueNotEnforcedDeclaration unique_p(dt);

    ASTPtr new_node = nullptr;

    if (dt.parse_mysql_ddl)
    {
        // mysql table_constraints
        if (s_index.checkWithoutMoving(pos, expected) || s_key.checkWithoutMoving(pos, expected) ||
            s_cluster_key.checkWithoutMoving(pos, expected) || s_primary_key.checkWithoutMoving(pos, expected) ||
            s_unique.checkWithoutMoving(pos, expected))
        {
            MySQLParser::ParserDeclareIndex index_p_mysql;
            if (index_p_mysql.parse(pos, new_node, expected))
            {
                node = new_node;
                return true;
            }
        }
    }

    if (s_index.ignore(pos, expected))
    {
        if (!index_p.parse(pos, new_node, expected))
            return false;
    }
    else if (s_projection.ignore(pos, expected))
    {
        if (!projection_p.parse(pos, new_node, expected))
            return false;
    }
    else if (s_primary_key.ignore(pos, expected))
    {
        if (!primary_key_p.parse(pos, new_node, expected))
            return false;
    }
    else if (s_constraint.ignore(pos, expected))
    {
        if (!foreign_key_p.parse(pos, new_node, expected) && !constraint_p.parse(pos, new_node, expected)
            && !unique_p.parse(pos, new_node, expected))
        {
            /// mysql case: constraint primary key
            MySQLParser::ParserDeclareIndex index_p_mysql;
            if (dt.parse_mysql_ddl && index_p_mysql.parse(pos, new_node, expected))
            {
                node = new_node;
                return true;
            }
            else
            {
                return false;
            }
        }
    }
    else if (s_foreign_key.checkWithoutMoving(pos, expected))
    {
        if (!foreign_key_p.parse(pos, new_node, expected))
            return false;
    }
    else if (s_bitengine_constraint.ignore(pos, expected))
    {
        if (!bitengine_constraint_p.parse(pos, new_node, expected))
            return false;
    }
    else
    {
        if (!column_p.parse(pos, new_node, expected))
            return false;
    }

    node = new_node;
    return true;
}

bool ParserIndexDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserIndexDeclaration>(dt), std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, node, expected);
}

bool ParserConstraintDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserConstraintDeclaration>(dt), std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, node, expected);
}

bool ParserBitEngineConstraintDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserBitEngineConstraintDeclaration>(dt), std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, node, expected);
}

bool ParserForeignKeyDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(
               std::make_unique<ParserForeignKeyDeclaration>(dt), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserUniqueNotEnforcedDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(
               std::make_unique<ParserUniqueNotEnforcedDeclaration>(dt),
               std::make_unique<ParserToken>(TokenType::Comma),
               false)
        .parse(pos, node, expected);
}

bool ParserProjectionDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserProjectionDeclaration>(dt), std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, node, expected);
}

bool ParserTablePropertiesDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list;
    if (!ParserList(
            std::make_unique<ParserTablePropertyDeclaration>(dt),
                    std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, list, expected))
        return false;

    ASTPtr columns = std::make_shared<ASTExpressionList>();
    ASTPtr indices = std::make_shared<ASTExpressionList>();
    ASTPtr mysql_indices = std::make_shared<ASTExpressionList>();
    ASTPtr constraints = std::make_shared<ASTExpressionList>();
    ASTPtr projections = std::make_shared<ASTExpressionList>();
    ASTPtr primary_key;
    ASTPtr foreign_keys = std::make_shared<ASTExpressionList>();
    ASTPtr unique = std::make_shared<ASTExpressionList>();

    for (const auto & elem : list->children)
    {
        if (elem->as<ASTColumnDeclaration>())
            columns->children.push_back(elem);
        else if (elem->as<ASTIndexDeclaration>())
            indices->children.push_back(elem);
        else if (elem->as<ASTConstraintDeclaration>())
            constraints->children.push_back(elem);
        else if (elem->as<ASTBitEngineConstraintDeclaration>())
            constraints->children.push_back(elem);
        else if (elem->as<ASTProjectionDeclaration>())
            projections->children.push_back(elem);
        else if (elem->as<ASTIdentifier>() || elem->as<ASTFunction>())
        {
            if (primary_key)
            {
                /// Multiple primary keys are not allowed.
                return false;
            }
            primary_key = elem;
        }
        else if (elem->as<ASTForeignKeyDeclaration>())
            foreign_keys->children.push_back(elem);
        else if (elem->as<ASTUniqueNotEnforcedDeclaration>())
            unique->children.push_back(elem);
        else if (elem->as<MySQLParser::ASTDeclareIndex>())
            mysql_indices->children.push_back(elem);
        else
            return false;
    }

    auto res = std::make_shared<ASTColumns>();

    if (!columns->children.empty())
        res->set(res->columns, columns);
    if (!indices->children.empty())
        res->set(res->indices, indices);
    if (!constraints->children.empty())
        res->set(res->constraints, constraints);
    if (!projections->children.empty())
        res->set(res->projections, projections);
    if (primary_key)
        res->set(res->primary_key, primary_key);
    if (!foreign_keys->children.empty())
        res->set(res->foreign_keys, foreign_keys);
    if (!unique->children.empty())
        res->set(res->unique, unique);
    if (!mysql_indices->children.empty())
        res->set(res->mysql_indices, mysql_indices);

    node = res;

    return true;
}


bool ParserStorage::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_engine("ENGINE");
    ParserToken s_eq(TokenType::Equals);
    ParserKeyword s_partition_by("PARTITION BY");
    ParserKeyword s_cluster_by("CLUSTER BY");
    ParserKeyword s_primary_key("PRIMARY KEY");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_unique_key("UNIQUE KEY");
    ParserKeyword s_sample_by("SAMPLE BY");
    ParserKeyword s_ttl("TTL");
    ParserKeyword s_settings("SETTINGS");

    ParserIdentifierWithOptionalParameters ident_with_optional_params_p(dt);
    ParserExpression expression_p(dt);
    ParserClusterByElement cluster_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ParserTTLExpressionList parser_ttl_list(dt);
    ParserStringLiteral string_literal_parser;

    ASTPtr engine;
    ASTPtr partition_by;
    ASTPtr cluster_by;
    ASTPtr primary_key;
    ASTPtr order_by;
    ASTPtr unique_key;
    ASTPtr sample_by;
    ASTPtr ttl_table;
    ASTPtr settings;

    if (!s_engine.ignore(pos, expected))
        return false;

    s_eq.ignore(pos, expected);

    if (!ident_with_optional_params_p.parse(pos, engine, expected))
        return false;

    while (true)
    {
        if (!partition_by && s_partition_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, partition_by, expected))
                continue;
            else
                return false;
        }

        if (!cluster_by && s_cluster_by.ignore(pos, expected))
        {
            if (cluster_p.parse(pos, cluster_by, expected))
                continue;
            else
                return false;
        }

        if (!primary_key && s_primary_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, primary_key, expected))
                continue;
            else
                return false;
        }

        if (!order_by && s_order_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, order_by, expected))
                continue;
            else
                return false;
        }


        if (!unique_key && s_unique_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, unique_key, expected))
                continue;
            else
                return false;
        }

        if (!sample_by && s_sample_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, sample_by, expected))
                continue;
            else
                return false;
        }

        if (!ttl_table && s_ttl.ignore(pos, expected))
        {
            if (parser_ttl_list.parse(pos, ttl_table, expected))
                continue;
            else
                return false;
        }

        if (s_settings.ignore(pos, expected))
        {
            if (!settings_p.parse(pos, settings, expected))
                return false;
        }

        break;
    }

    if (engine)
    {
        switch (engine_kind)
        {
            case EngineKind::TABLE_ENGINE:
                engine->as<ASTFunction &>().kind = ASTFunction::Kind::TABLE_ENGINE;
                break;

            case EngineKind::DATABASE_ENGINE:
                engine->as<ASTFunction &>().kind = ASTFunction::Kind::DATABASE_ENGINE;
                break;
        }
    }

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, engine);
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->cluster_by, cluster_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
    storage->set(storage->unique_key, unique_key);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->ttl_table, ttl_table);

    storage->set(storage->settings, settings);

    node = storage;
    return true;
}

bool ParserStorageMySQL::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_distributed_by("DISTRIBUTED BY");
    ParserKeyword s_distribute_by("DISTRIBUTE BY");
    ParserKeyword s_hash("HASH");
    ParserToken s_eq(TokenType::Equals);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserKeyword s_broadcast("BROADCAST");
    ParserKeyword s_partition_by("PARTITION BY");
    ParserKeyword s_partitions("PARTITIONS");
    ParserKeyword s_value("VALUE");
    ParserKeyword s_lifecycle("LIFECYCLE");
    ParserKeyword s_storage_policy("STORAGE_POLICY");
    ParserKeyword s_block_size("BLOCK_SIZE");
    ParserKeyword s_engine("ENGINE");
    ParserKeyword s_rt_engine("RT_ENGINE");
    ParserKeyword s_table_properties("TABLE_PROPERTIES");
    ParserKeyword s_hot_partition_count("HOT_PARTITION_COUNT");
    ParserKeyword s_index_all("INDEX_ALL");

    ParserKeyword s_cluster_by("CLUSTER BY");
    ParserKeyword s_primary_key("PRIMARY KEY");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_unique_key("UNIQUE KEY");
    ParserKeyword s_sample_by("SAMPLE BY");
    ParserKeyword s_ttl("TTL");
    ParserKeyword s_settings("SETTINGS");

    ParserKeyword s_charset1("CHARSET");
    ParserKeyword s_default_charset1("DEFAULT CHARSET");
    ParserKeyword s_charset2("CHARACTER SET");
    ParserKeyword s_default_charset2("DEFAULT CHARACTER SET");
    ParserKeyword s_collate("COLLATE");
    ParserKeyword s_default_collate("DEFAULT COLLATE");
    ParserKeyword s_auto_increment("AUTO_INCREMENT");
    ParserKeyword s_row_format("ROW_FORMAT");
    ParserKeyword s_checksum("CHECKSUM");

    ParserIdentifierWithOptionalParameters ident_with_optional_params_p(dt);
    ParserExpression expression_p(dt);
    ParserClusterByElement cluster_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ParserTTLExpressionList parser_ttl_list(dt);
    ParserStringLiteral string_literal_parser;
    ParserNumber number_parser(dt);

    ASTPtr engine;
    ASTPtr cluster_by;
    ASTPtr primary_key;
    ASTPtr order_by;
    ASTPtr unique_key;
    ASTPtr sample_by;
    ASTPtr ttl_table;
    ASTPtr settings;

    // MySQL specific
    ASTPtr distributed_by;
    ASTPtr storage_policy;
    ASTPtr hot_partition_count;
    ASTPtr block_size;
    ASTPtr mysql_engine;
    ASTPtr rt_engine;
    ASTPtr table_properties;
    ASTPtr mysql_partition_by;
    ASTPtr life_cycle;
    ASTPtr charset;
    ASTPtr collate;
    ASTPtr auto_increment;
    ASTPtr row_format;
    ASTPtr check_sum;
    ASTPtr index_all;
    bool broadcast = false;

    // optional engine
    if (s_engine.ignore(pos, expected))
    {
        s_eq.ignore(pos, expected);

        if (!ident_with_optional_params_p.parse(pos, engine, expected) && !string_literal_parser.parse(pos, mysql_engine, expected))
            return false;
    }

    while (true)
    {
        if (!distributed_by && (s_distributed_by.ignore(pos, expected) || s_distribute_by.ignore(pos, expected)))
        {
            if (s_hash.ignore(pos, expected))
            {
                if (!expression_p.parse(pos, distributed_by, expected))
                    return false;
            }
            else if (s_broadcast.ignore(pos, expected))
                broadcast = true;
            else
                return false;
        }

        if (!mysql_partition_by && s_partition_by.ignore(pos, expected))
        {
            ParserKeyword("KEY").ignore(pos, expected);

            if (s_value.ignore(pos, expected) && expression_p.parse(pos, mysql_partition_by, expected))
            {
                // partition by value(expr) life cycle n
                if (s_lifecycle.ignore(pos, expected) && number_parser.parse(pos, life_cycle, expected))
                    continue;
                // not enforce lifecycle n
                continue;
            }
            else if (expression_p.parse(pos, mysql_partition_by, expected))
                continue;
            else
                return false;
        }

        if (dt.parse_mysql_ddl && s_partitions.ignore(pos, expected))
        {
            if (!number_parser.ignore(pos, expected))
                return false;
        }

        if (!storage_policy && s_storage_policy.ignore(pos, expected))
        {
            if (!s_eq.ignore(pos, expected))
                return false;

            if (!string_literal_parser.parse(pos, storage_policy, expected))
                return false;

            if (s_hot_partition_count.ignore(pos, expected))
            {
                if (!s_eq.ignore(pos, expected))
                    return false;
                if (number_parser.parse(pos, hot_partition_count, expected))
                    continue;
                else
                    return false;
            }
        }

        if (!block_size && s_block_size.ignore(pos, expected))
        {
            if (!s_eq.ignore(pos, expected))
                return false;
            if (number_parser.parse(pos, block_size, expected))
                continue;
            else
                return false;
        }

        if (!engine && s_engine.ignore(pos, expected))
        {
            s_eq.ignore(pos, expected);

            if (ident_with_optional_params_p.parse(pos, engine, expected) || string_literal_parser.parse(pos, mysql_engine, expected))
                continue;
            else
                return false;
        }

        if (!rt_engine && s_rt_engine.ignore(pos, expected))
        {
            if (!s_eq.ignore(pos, expected))
                return false;
            if (string_literal_parser.parse(pos, rt_engine, expected))
                continue;
            else
                return false;
        }

        if (!table_properties && s_table_properties.ignore(pos, expected))
        {
            if (!s_eq.ignore(pos, expected))
                return false;
            if (string_literal_parser.parse(pos, table_properties, expected))
                continue;
            else
                return false;
        }

        if (!charset && (s_charset1.ignore(pos, expected) || s_default_charset1.ignore(pos, expected) || s_charset2.ignore(pos, expected)
            || s_default_charset2.ignore(pos, expected)))
        {
            s_eq.ignore(pos, expected);
            if (expression_p.parse(pos, charset, expected))
                continue;
            else
                return false;
        }

        if (!collate && (s_collate.ignore(pos, expected) || s_default_collate.ignore(pos, expected)))
        {
            s_eq.ignore(pos, expected);
            if (expression_p.parse(pos, collate, expected))
                continue;
            else
                return false;
        }

        if (!auto_increment && s_auto_increment.ignore(pos, expected))
        {
            s_eq.ignore(pos, expected);
            if (expression_p.parse(pos, auto_increment, expected))
                continue;
            else
                return false;
        }

        if (!check_sum && s_checksum.ignore(pos, expected))
        {
            s_eq.ignore(pos, expected);
            if (expression_p.parse(pos, check_sum, expected))
                continue;
            else
                return false;
        }

        if (!row_format && s_row_format.ignore(pos, expected))
        {
            s_eq.ignore(pos, expected);
            if (expression_p.parse(pos, row_format, expected))
                continue;
            else
                return false;
        }

        if (!index_all && s_index_all.ignore(pos, expected))
        {
            s_eq.ignore(pos, expected);
            if (expression_p.parse(pos, index_all, expected))
                continue;
            else
                return false;
        }

        if (!cluster_by && s_cluster_by.ignore(pos, expected))
        {
            if (cluster_p.parse(pos, cluster_by, expected))
                continue;
            else
                return false;
        }

        if (!primary_key && s_primary_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, primary_key, expected))
                continue;
            else
                return false;
        }

        if (!order_by && s_order_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, order_by, expected))
                continue;
            else
                return false;
        }


        if (!unique_key && s_unique_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, unique_key, expected))
                continue;
            else
                return false;
        }

        if (!sample_by && s_sample_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, sample_by, expected))
                continue;
            else
                return false;
        }

        if (!ttl_table && s_ttl.ignore(pos, expected))
        {
            if (parser_ttl_list.parse(pos, ttl_table, expected))
                continue;
            else
                return false;
        }

        if (s_settings.ignore(pos, expected))
        {
            if (!settings_p.parse(pos, settings, expected))
                return false;
        }

        break;
    }

    auto storage = std::make_shared<ASTStorageAnalyticalMySQL>();
    storage->set(storage->engine, engine);
    storage->set(storage->cluster_by, cluster_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
    storage->set(storage->unique_key, unique_key);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->ttl_table, ttl_table);

    storage->set(storage->settings, settings);

    storage->set(storage->mysql_engine, mysql_engine);
    storage->set(storage->distributed_by, distributed_by);
    storage->set(storage->storage_policy, storage_policy);
    storage->set(storage->hot_partition_count, hot_partition_count);
    storage->set(storage->block_size, block_size);
    storage->set(storage->rt_engine, rt_engine);
    storage->set(storage->table_properties, table_properties);
    storage->set(storage->mysql_partition_by, mysql_partition_by);
    storage->set(storage->life_cycle, life_cycle);
    storage->broadcast = broadcast;

    node = storage;
    return true;
}

bool ParserCreateTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_replace("REPLACE");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserCompoundIdentifier table_name_p(true);
    ParserKeyword s_from("FROM");
    ParserKeyword s_on("ON");
    ParserKeyword s_as("AS");
    ParserKeyword s_like("LIKE");
    ParserKeyword s_ignore("IGNORE");
    ParserKeyword s_replicated("REPLICATED");
    ParserKeyword s_async("ASYNC");
    ParserKeyword s_bitengine_encode("BITENGINEENCODE");
    ParserKeyword s_ttl("TTL");
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p(ParserStorage::TABLE_ENGINE, dt);
    ParserIdentifier name_p;
    ParserTablePropertiesDeclarationList table_properties_p(dt);
    ParserSelectWithUnionQuery select_p(dt);
    ParserFunction table_function_p(dt);
    ParserNameList names_p(dt);

    ASTPtr table;
    ASTPtr columns_list;
    ASTPtr storage;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr as_table_function;
    ASTPtr select;
    ASTPtr from_path;

    String cluster_str;
    bool attach = false;
    bool replace = false;
    bool or_replace = false;
    bool if_not_exists = false;
    bool is_temporary = false;
    bool ignore_replicated = false;
    bool ignore_async = false;
    bool ignore_bitengine_encode = false;
    bool ignore_ttl = false;

    if (s_create.ignore(pos, expected))
    {
        if (s_or_replace.ignore(pos, expected))
            replace = or_replace = true;
    }
    else if (s_attach.ignore(pos, expected))
        attach = true;
    else if (s_replace.ignore(pos, expected))
        replace = true;
    else
        return false;


    if (!replace && !or_replace && s_temporary.ignore(pos, expected))
    {
        is_temporary = true;
    }
    if (!s_table.ignore(pos, expected))
        return false;

    if (!replace && !or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;
    tryRewriteCnchDatabaseName(table, pos.getContext());


    if (attach && s_from.ignore(pos, expected))
    {
        ParserLiteral from_path_p(dt);
        if (!from_path_p.parse(pos, from_path, expected))
            return false;
    }

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto table_id = table->as<ASTTableIdentifier>()->getTableId();

    // Shortcut for ATTACH a previously detached table
    bool short_attach = attach && !from_path;
    if (short_attach && (!pos.isValid() || pos.get().type == TokenType::Semicolon))
    {
        auto query = std::make_shared<ASTCreateQuery>();
        node = query;

        query->attach = attach;
        query->if_not_exists = if_not_exists;
        query->cluster = cluster_str;
        query->setTableInfo(table_id);
        // query->catalog = table_id.catalog_name;
        // query->database = table_id.database_name;
        // query->table = table_id.table_name;
        query->uuid = table_id.uuid;

        return true;
    }

    /// List of columns.
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;

        auto storage_parse_result = storage_p.parse(pos, storage, expected);

        if (storage_parse_result && s_as.ignore(pos, expected))
        {
            if (!select_p.parse(pos, select, expected))
                return false;
        }

        if (!storage_parse_result && !is_temporary)
        {
            if (!s_as.ignore(pos, expected))
                return false;
            if (!table_function_p.parse(pos, as_table_function, expected))
            {
                return false;
            }
        }
    }
    /** Create queries without list of columns:
      *  - CREATE|ATTACH TABLE ... AS ...
      *  - CREATE|ATTACH TABLE ... ENGINE = engine
      */
    else
    {
        storage_p.parse(pos, storage, expected);

        if (s_as.ignore(pos, expected) && !select_p.parse(pos, select, expected)) /// AS SELECT ...
        {
            /// ENGINE can not be specified for table functions.
            if (storage || !table_function_p.parse(pos, as_table_function, expected))
            {
                /// AS [db.]table
                if (!name_p.parse(pos, as_table, expected))
                    return false;

                if (s_dot.ignore(pos, expected))
                {
                    as_database = as_table;
                    tryRewriteCnchDatabaseName(as_database, pos.getContext());
                    if (!name_p.parse(pos, as_table, expected))
                        return false;
                }

                /// Optional - IGNORE REPLICATE can be specified
                if (s_ignore.ignore(pos, expected))
                {
                    bool option = false;
                    do
                    {
                        option = false;
                        bool temp = s_replicated.ignore(pos, expected);
                        if (temp) option = ignore_replicated = true;
                        temp = s_async.ignore(pos, expected);
                        if (temp) option = ignore_async = true;
                        temp = s_ttl.ignore(pos, expected);
                        if (temp) option = ignore_ttl = true;
                        option |= s_bitengine_encode.ignore(pos, expected);

                        // As for online tmp table created by 'CREATE TABLE tabl_x as tabl_y IGNORE ...',
                        // this attribute is always set true for not encoding a tmp table so as to
                        // protect the local table away from a wrong dictionary
                        ignore_bitengine_encode = true;

                    } while (option);

                    if (!ignore_replicated && !ignore_async && !ignore_ttl)
                        return false;
                }

                /// Optional - ENGINE can be specified.
                if (!storage)
                    storage_p.parse(pos, storage, expected);
            }
        }
        else if (s_like.ignore(pos, expected))
        {
            /// LIKE [db.]table
            if (!name_p.parse(pos, as_table, expected))
                return false;

            if (s_dot.ignore(pos, expected))
            {
                as_database = as_table;
                tryRewriteCnchDatabaseName(as_database, pos.getContext());
                if (!name_p.parse(pos, as_table, expected))
                    return false;
            }
        }
    }

    auto comment = parseComment(pos, expected);

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    if (as_table_function)
        query->as_table_function = as_table_function;

    query->attach = attach;
    query->replace_table = replace;
    query->create_or_replace = or_replace;
    query->if_not_exists = if_not_exists;
    query->temporary = is_temporary;
    query->ignore_replicated = ignore_replicated;
    query->ignore_async = ignore_async;
    query->ignore_bitengine_encode = ignore_bitengine_encode;
    query->ignore_ttl = ignore_ttl;

    query->setTableInfo(table_id);
    // query->catalog = table_id.catalog_name;
    // query->database = table_id.database_name;
    // query->table = table_id.table_name;
    // query->uuid = table_id.uuid;
    query->cluster = cluster_str;

    query->set(query->columns_list, columns_list);
    query->set(query->storage, storage);

    if (comment)
        query->set(query->comment, comment);

    if (query->storage && query->columns_list && query->columns_list->primary_key)
    {
        if (query->storage->primary_key)
        {
            throw Exception("Multiple primary keys are not allowed.", ErrorCodes::BAD_ARGUMENTS);
        }
        query->storage->primary_key = query->columns_list->primary_key;
    }

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);

    if (from_path)
        query->attach_from_path = from_path->as<ASTLiteral &>().value.get<String>();

    return true;
}

bool ParserCreateTableAnalyticalMySQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserCompoundIdentifier table_name_p(true);
    ParserKeyword s_on("ON");
    ParserKeyword s_as("AS");
    ParserKeyword s_like("LIKE");
    ParserKeyword s_ignore("IGNORE");
    ParserKeyword s_replicated("REPLICATED");
    ParserKeyword s_async("ASYNC");
    ParserKeyword s_bitengine_encode("BITENGINEENCODE");
    ParserKeyword s_ttl("TTL");
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorageMySQL storage_p(dt);
    ParserIdentifier name_p;
    ParserTablePropertiesDeclarationList table_properties_p(dt);
    ParserSelectWithUnionQuery select_p(dt);
    ParserFunction table_function_p(dt);
    ParserNameList names_p(dt);

    ASTPtr table;
    ASTPtr columns_list;
    ASTPtr storage;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr as_table_function;
    ASTPtr select;
    ASTPtr from_path;

    String cluster_str;
    bool attach = false;
    bool replace = false;
    bool or_replace = false;
    bool if_not_exists = false;
    bool is_temporary = false;
    bool ignore_replicated = false;
    bool ignore_async = false;
    bool ignore_bitengine_encode = false;
    bool ignore_ttl = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_table.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;
    tryRewriteCnchDatabaseName(table, pos.getContext());

    auto table_id = table->as<ASTTableIdentifier>()->getTableId();

    /// List of columns.
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;

        auto storage_parse_result = storage_p.parse(pos, storage, expected);

        if (storage_parse_result && s_as.ignore(pos, expected))
        {
            if (!select_p.parse(pos, select, expected))
                return false;
        }

        if (!storage_parse_result && !is_temporary)
        {
            if (!s_as.ignore(pos, expected))
                return false;
            if (!table_function_p.parse(pos, as_table_function, expected))
            {
                return false;
            }
        }
    }
    /** Create queries without list of columns:
      *  - CREATE|ATTACH TABLE ... AS ...
      *  - CREATE|ATTACH TABLE ... ENGINE = engine
      */
    else
    {
        storage_p.parse(pos, storage, expected);

        if (s_as.ignore(pos, expected) && !select_p.parse(pos, select, expected)) /// AS SELECT ...
        {
            /// ENGINE can not be specified for table functions.
            if (storage || !table_function_p.parse(pos, as_table_function, expected))
            {
                /// AS [db.]table
                if (!name_p.parse(pos, as_table, expected))
                    return false;

                if (s_dot.ignore(pos, expected))
                {
                    as_database = as_table;
                    tryRewriteCnchDatabaseName(as_database, pos.getContext());
                    if (!name_p.parse(pos, as_table, expected))
                        return false;
                }

                /// Optional - IGNORE REPLCIATE can be specified
                if (s_ignore.ignore(pos, expected))
                {
                    bool option = false;
                    do
                    {
                        option = false;
                        bool temp = s_replicated.ignore(pos, expected);
                        if (temp) option = ignore_replicated = true;
                        temp = s_async.ignore(pos, expected);
                        if (temp) option = ignore_async = true;
                        temp = s_ttl.ignore(pos, expected);
                        if (temp) option = ignore_ttl = true;
                        temp = s_bitengine_encode.ignore(pos, expected);
                        if (temp)
                            option = ignore_bitengine_encode = true;

                    } while (option);

                    if (!ignore_replicated && !ignore_async && !ignore_ttl && !ignore_bitengine_encode)
                        return false;
                }

                /// Optional - ENGINE can be specified.
                if (!storage)
                    storage_p.parse(pos, storage, expected);
            }
        }
        else if (s_like.ignore(pos, expected)) {
            /// LIKE [db.]table
            if (!name_p.parse(pos, as_table, expected))
                return false;

            if (s_dot.ignore(pos, expected))
            {
                as_database = as_table;
                tryRewriteCnchDatabaseName(as_database, pos.getContext());
                if (!name_p.parse(pos, as_table, expected))
                    return false;
            }
        }
        else if (!select && !select_p.parse(pos, select, expected))   // CREATE ... SELECT ...
        {
            return false;
        }
    }

    auto comment = parseComment(pos, expected);

    auto query = std::make_shared<ASTCreateQueryAnalyticalMySQL>();
    node = query;

    if (as_table_function)
        query->as_table_function = as_table_function;

    query->attach = attach;
    query->replace_table = replace;
    query->create_or_replace = or_replace;
    query->if_not_exists = if_not_exists;
    query->temporary = is_temporary;
    query->ignore_replicated = ignore_replicated;
    query->ignore_async = ignore_async;
    query->ignore_bitengine_encode = ignore_bitengine_encode;
    query->ignore_ttl = ignore_ttl;

    query->setTableInfo(table_id);
    // query->catalog = table_id.catalog_name;
    // query->database = table_id.database_name;
    // query->table = table_id.table_name;
    // query->uuid = table_id.uuid;
    query->cluster = cluster_str;

    query->set(query->columns_list, columns_list);
    query->set(query->storage, storage);

    if (comment)
        query->set(query->comment, comment);

    if (query->storage && query->columns_list && query->columns_list->primary_key)
    {
        if (query->storage->primary_key)
        {
            throw Exception("Multiple primary keys are not allowed.", ErrorCodes::BAD_ARGUMENTS);
        }
        query->storage->primary_key = query->columns_list->primary_key;
    }

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);

    if (from_path)
        query->attach_from_path = from_path->as<ASTLiteral &>().value.get<String>();

    return true;
}

bool ParserCreateLiveViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserCompoundIdentifier table_name_p(true);
    ParserKeyword s_as("AS");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_live("LIVE");
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserTablePropertiesDeclarationList table_properties_p(dt);
    ParserSelectWithUnionQuery select_p(dt);

    ASTPtr table;
    ASTPtr to_table;
    ASTPtr columns_list;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr select;
    ASTPtr live_view_timeout;
    ASTPtr live_view_periodic_refresh;

    String cluster_str;
    bool attach = false;
    bool if_not_exists = false;
    bool with_and = false;
    bool with_timeout = false;
    bool with_periodic_refresh = false;

    if (!s_create.ignore(pos, expected))
    {
        if (s_attach.ignore(pos, expected))
            attach = true;
        else
            return false;
    }

    if (!s_live.ignore(pos, expected))
        return false;

    if (!s_view.ignore(pos, expected))
       return false;

    if (s_if_not_exists.ignore(pos, expected))
       if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;
    tryRewriteCnchDatabaseName(table, pos.getContext());

    if (ParserKeyword{"WITH"}.ignore(pos, expected))
    {
        if (ParserKeyword{"TIMEOUT"}.ignore(pos, expected))
        {
            if (!ParserNumber{dt}.parse(pos, live_view_timeout, expected))
            {
                live_view_timeout = std::make_shared<ASTLiteral>(static_cast<UInt64>(DEFAULT_TEMPORARY_LIVE_VIEW_TIMEOUT_SEC));
            }

            /// Optional - AND
            if (ParserKeyword{"AND"}.ignore(pos, expected))
                with_and = true;

            with_timeout = true;
        }

        if (ParserKeyword{"REFRESH"}.ignore(pos, expected) || ParserKeyword{"PERIODIC REFRESH"}.ignore(pos, expected))
        {
            if (!ParserNumber{dt}.parse(pos, live_view_periodic_refresh, expected))
                live_view_periodic_refresh = std::make_shared<ASTLiteral>(static_cast<UInt64>(DEFAULT_PERIODIC_LIVE_VIEW_REFRESH_SEC));

            with_periodic_refresh = true;
        }

        else if (with_and)
            return false;

        if (!with_timeout && !with_periodic_refresh)
            return false;
    }

    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    // TO [db.]table
    if (ParserKeyword{"TO"}.ignore(pos, expected))
    {
        if (!table_name_p.parse(pos, to_table, expected))
            return false;
        tryRewriteCnchDatabaseName(to_table, pos.getContext());
    }

    /// Optional - a list of columns can be specified. It must fully comply with SELECT.
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    /// AS SELECT ...
    if (!s_as.ignore(pos, expected))
        return false;

    if (!select_p.parse(pos, select, expected))
        return false;

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_live_view = true;

    auto table_id = table->as<ASTTableIdentifier>()->getTableId();
    query->setTableInfo(table_id);
    // query->catalog = table_id.database_name;
    // query->database = table_id.database_name;
    // query->table = table_id.table_name;
    // query->uuid = table_id.uuid;
    query->cluster = cluster_str;

    if (to_table)
        query->to_table_id = to_table->as<ASTTableIdentifier>()->getTableId();

    query->set(query->columns_list, columns_list);

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);

    if (live_view_timeout)
        query->live_view_timeout.emplace(live_view_timeout->as<ASTLiteral &>().value.safeGet<UInt64>());

    if (live_view_periodic_refresh)
        query->live_view_periodic_refresh.emplace(live_view_periodic_refresh->as<ASTLiteral &>().value.safeGet<UInt64>());

    return true;
}

bool ParserTableOverrideDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_table_override("TABLE OVERRIDE");
    ParserIdentifier table_name_p;
    ParserToken lparen_p(TokenType::OpeningRoundBracket);
    ParserToken rparen_p(TokenType::ClosingRoundBracket);
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserExpression expression_p;
    ParserTTLExpressionList parser_ttl_list;
    ParserKeyword s_columns("COLUMNS");
    ParserKeyword s_partition_by("PARTITION BY");
    ParserKeyword s_primary_key("PRIMARY KEY");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_unique_key("UNIQUE KEY");
    ParserKeyword s_sample_by("SAMPLE BY");
    ParserKeyword s_ttl("TTL");
    ASTPtr table_name;
    ASTPtr columns;
    ASTPtr partition_by;
    ASTPtr primary_key;
    ASTPtr order_by;
    ASTPtr unique_key;
    ASTPtr sample_by;
    ASTPtr ttl_table;

    if (!s_table_override.ignore(pos, expected))
        return false;

    if (!table_name_p.parse(pos, table_name, expected))
        return false;

    if (!lparen_p.ignore(pos, expected))
        return false;

    while (true)
    {
        if (!columns && s_columns.ignore(pos, expected))
        {
            if (!lparen_p.ignore(pos, expected))
                return false;
            if (!table_properties_p.parse(pos, columns, expected))
                return false;
            if (!rparen_p.ignore(pos, expected))
                return false;
        }


        if (!partition_by && s_partition_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, partition_by, expected))
                continue;
            else
                return false;
        }

        if (!primary_key && s_primary_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, primary_key, expected))
                continue;
            else
                return false;
        }

        if (!order_by && s_order_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, order_by, expected))
                continue;
            else
                return false;
        }

        if (!unique_key && s_unique_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, unique_key, expected))
                continue;
            else
                return false;
        }

        if (!sample_by && s_sample_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, sample_by, expected))
                continue;
            else
                return false;
        }

        if (!ttl_table && s_ttl.ignore(pos, expected))
        {
            if (parser_ttl_list.parse(pos, ttl_table, expected))
                continue;
            else
                return false;
        }

        break;
    }

    if (!rparen_p.ignore(pos, expected))
        return false;

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
    storage->set(storage->unique_key, unique_key);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->ttl_table, ttl_table);

    auto res = std::make_shared<ASTTableOverride>();
    res->table_name = table_name->as<ASTIdentifier>()->name();
    res->set(res->storage, storage);
    if (columns)
        res->set(res->columns, columns);

    node = res;

    return true;
}

bool ParserTableOverridesDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserTableOverrideDeclaration table_override_p;
    ParserToken s_comma(TokenType::Comma);
    auto res = std::make_shared<ASTTableOverrideList>();
    auto parse_element = [&]
    {
        ASTPtr element;
        if (!table_override_p.parse(pos, element, expected))
            return false;
        auto * table_override = element->as<ASTTableOverride>();
        if (!table_override)
            return false;
        res->setTableOverride(table_override->table_name, element);
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_element, s_comma, true))
        return false;

    if (!res->children.empty())
        node = res;

    return true;
}

bool ParserCreateDatabaseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_database("DATABASE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserStorage storage_p(ParserStorage::DATABASE_ENGINE, dt);
    ParserIdentifier name_p;
    ParserTableOverridesDeclarationList table_overrides_p;
    ParserIdentifier id_p;

    ParserKeyword s_schema("SCHEMA");
    ParserKeyword s_default("DEFAULT");
    ParserKeyword s_character_set("CHARACTER SET");
    ParserKeyword s_collate("COLLATE");

    ASTPtr database;
    ASTPtr storage;
    ASTPtr table_overrides;
    UUID uuid = UUIDHelpers::Nil;
    ASTPtr character_set;
    ASTPtr collate;

    String cluster_str;
    bool attach = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
    {
        if (s_attach.ignore(pos, expected))
            attach = true;
        else
            return false;
    }

    if (!s_database.ignore(pos, expected) && !s_schema.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!name_p.parse(pos, database, expected))
        return false;
    tryRewriteCnchDatabaseName(database, pos.getContext());

    if (ParserKeyword("UUID").ignore(pos, expected))
    {
        ParserStringLiteral uuid_p;
        ASTPtr ast_uuid;
        if (!uuid_p.parse(pos, ast_uuid, expected))
            return false;
        uuid = parseFromString<UUID>(ast_uuid->as<ASTLiteral>()->value.get<String>());
    }

    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    s_default.ignore(pos, expected);
    if (s_character_set.ignore(pos, expected))
    {
        if (!id_p.parse(pos, character_set, expected))
            return false;
    }
    s_default.ignore(pos, expected);
    if (s_collate.ignore(pos, expected))
    {
        if (!id_p.parse(pos, collate, expected))
                return false;
    }

    storage_p.parse(pos, storage, expected);
    auto comment = parseComment(pos, expected);

    if (!table_overrides_p.parse(pos, table_overrides, expected))
        return false;

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;

    tryGetIdentifierNameInto(database, query->database);
    query->uuid = uuid;
    query->cluster = cluster_str;

    query->set(query->storage, storage);
    if (comment)
        query->set(query->comment, comment);

    if (table_overrides && !table_overrides->children.empty())
        query->set(query->table_overrides, table_overrides);

    return true;
}

bool ParserCreateCatalogQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_external("EXTERNAL");
    ParserKeyword s_catalog("CATALOG");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserIdentifier name_p;
    ParserKeyword s_properties("PROPERTIES");
    ParserToken s_left_paren(TokenType::OpeningRoundBracket);
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ParserToken s_right_paren(TokenType::ClosingRoundBracket);

    ASTPtr catalog;
    ASTPtr properties;
    bool if_not_exists = false;

    if(!s_create.ignore(pos,expected))
    {
        return false;
    }
    if (!s_external.ignore(pos, expected))
    {
        return false;
    }
    if (!s_catalog.ignore(pos, expected))
    {
        return false;
    }
    if (s_if_not_exists.ignore(pos, expected))
    {
        if_not_exists = true;
    }
    if (!name_p.parse(pos, catalog, expected))
    {
        return false;
    }
    if (!s_properties.ignore(pos, expected))
    {
        return false;
    }
    if (!settings_p.parse(pos, properties, expected))
    {
        return false;
    }
    tryRewriteHiveCatalogName(catalog, pos.getContext());
    auto query = std::make_shared<ASTCreateQuery>();
    node = query;
    query->if_not_exists = if_not_exists;
    query->set(query->catalog_properties, properties);
    tryGetIdentifierNameInto(catalog, query->catalog);
    return true;
}


bool ParserCreateViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserCompoundIdentifier table_name_p(true);
    ParserKeyword s_as("AS");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_materialized("MATERIALIZED");
    ParserKeyword s_populate("POPULATE");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p(ParserStorage::TABLE_ENGINE, dt);
    ParserIdentifier name_p;
    ParserTablePropertiesDeclarationList table_properties_p(dt);
    ParserSelectWithUnionQuery select_p(dt);
    ParserNameList names_p(dt);

    ASTPtr table;
    ASTPtr to_table;
    ASTPtr to_inner_uuid;
    ASTPtr columns_list;
    ASTPtr storage;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr select;
    ASTPtr refresh_strategy;

    String cluster_str;
    bool attach = false;
    bool if_not_exists = false;
    bool is_ordinary_view = false;
    bool is_materialized_view = false;
    bool is_populate = false;
    bool replace_view = false;

    if (!s_create.ignore(pos, expected))
    {
        if (s_attach.ignore(pos, expected))
            attach = true;
        else
            return false;
    }

    /// VIEW or MATERIALIZED VIEW
    if (s_or_replace.ignore(pos, expected))
    {
        replace_view = true;
    }

    if (!replace_view && s_materialized.ignore(pos, expected))
    {
        is_materialized_view = true;
    }
    else
        is_ordinary_view = true;

    if (!s_view.ignore(pos, expected))
        return false;

    if (!replace_view && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;
    tryRewriteCnchDatabaseName(table, pos.getContext());

    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (ParserKeyword{"TO INNER UUID"}.ignore(pos, expected))
    {
        ParserLiteral literal_p(dt);
        if (!literal_p.parse(pos, to_inner_uuid, expected))
            return false;
    }
    else if (ParserKeyword{"TO"}.ignore(pos, expected))
    {
        // TO [db.]table
        if (!table_name_p.parse(pos, to_table, expected))
            return false;
        tryRewriteCnchDatabaseName(to_table, pos.getContext());
    }

    /// Optional - a list of columns can be specified. It must fully comply with SELECT.
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    if (is_materialized_view && !to_table)
    {
        /// Internal ENGINE for MATERIALIZED VIEW must be specified.
        if (!storage_p.parse(pos, storage, expected))
            return false;

        if (s_populate.ignore(pos, expected))
            is_populate = true;
    }

    if (ParserKeyword{"REFRESH"}.ignore(pos, expected))
    {
        // REFRESH only with materialized views
        if (!is_materialized_view)
            return false;
        if (!ParserRefreshStrategy{}.parse(pos, refresh_strategy, expected))
            return false;
    }

    /// AS SELECT ...
    if (!s_as.ignore(pos, expected))
        return false;

    if (!select_p.parse(pos, select, expected))
        return false;

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_ordinary_view = is_ordinary_view;
    query->is_materialized_view = is_materialized_view;
    query->is_populate = is_populate;
    query->replace_view = replace_view;

    auto table_id = table->as<ASTTableIdentifier>()->getTableId();
    query->database = table_id.database_name;
    query->table = table_id.table_name;
    query->uuid = table_id.uuid;
    query->cluster = cluster_str;

    if (to_table)
        query->to_table_id = to_table->as<ASTTableIdentifier>()->getTableId();
    if (to_inner_uuid)
        query->to_inner_uuid = parseFromString<UUID>(to_inner_uuid->as<ASTLiteral>()->value.get<String>());

    query->set(query->columns_list, columns_list);
    query->set(query->storage, storage);
    if (refresh_strategy)
        query->set(query->refresh_strategy, refresh_strategy);

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);

    return true;

}

bool ParserCreateDictionaryQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_replace("REPLACE");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_dictionary("DICTIONARY");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_on("ON");
    ParserCompoundIdentifier dict_name_p(true);
    ParserToken s_left_paren(TokenType::OpeningRoundBracket);
    ParserToken s_right_paren(TokenType::ClosingRoundBracket);
    ParserToken s_dot(TokenType::Dot);
    ParserDictionaryAttributeDeclarationList attributes_p(dt);
    ParserDictionary dictionary_p(dt);

    bool if_not_exists = false;
    bool replace = false;
    bool or_replace = false;

    ASTPtr name;
    ASTPtr attributes;
    ASTPtr dictionary;
    String cluster_str;

    bool attach = false;

    if (s_create.ignore(pos, expected))
    {
        if (s_or_replace.ignore(pos, expected))
        {
            replace = true;
            or_replace = true;
        }
    }
    else if (s_attach.ignore(pos, expected))
        attach = true;
    else if (s_replace.ignore(pos, expected))
        replace = true;
    else
        return false;

    if (!s_dictionary.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!dict_name_p.parse(pos, name, expected))
        return false;
    tryRewriteCnchDatabaseName(name, pos.getContext());

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (!attach)
    {
        if (!s_left_paren.ignore(pos, expected))
            return false;

        if (!attributes_p.parse(pos, attributes, expected))
            return false;

        if (!s_right_paren.ignore(pos, expected))
            return false;

        if (!dictionary_p.parse(pos, dictionary, expected))
            return false;
    }

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;
    query->is_dictionary = true;
    query->attach = attach;
    query->create_or_replace = or_replace;
    query->replace_table = replace;

    auto dict_id = name->as<ASTTableIdentifier>()->getTableId();
    query->database = dict_id.database_name;
    query->table = dict_id.table_name;
    query->uuid = dict_id.uuid;

    query->if_not_exists = if_not_exists;
    query->set(query->dictionary_attributes_list, attributes);
    query->set(query->dictionary, dictionary);
    query->cluster = cluster_str;

    return true;
}

bool ParserCreateSnapshotQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"CREATE SNAPSHOT"}.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    ASTPtr table;
    ASTPtr to_table;
    ASTPtr ttl_ast;
    Int64 ttl_in_days;

    if (ParserKeyword{"IF NOT EXISTS"}.ignore(pos, expected))
        if_not_exists = true;

    ParserCompoundIdentifier name_p(true);
    if (!name_p.parse(pos, table, expected))
        return false;

    if (ParserKeyword{"TO"}.ignore(pos, expected))
    {
        if (!name_p.parse(pos, to_table, expected))
            return false;
    }

    if (!ParserKeyword{"TTL"}.ignore(pos, expected))
        return false;

    if (!ParserUnsignedInteger{}.parse(pos, ttl_ast, expected))
        return false;

    ttl_in_days = ttl_ast->as<ASTLiteral &>().value.get<Int64>();
    if (ttl_in_days <= 0 || ttl_in_days > 365)
    {
        expected.add(pos, "ttl must be greater than 0 and smaller than 365");
        return false;
    }

    if (!ParserKeyword{"DAYS"}.ignore(pos, expected))
        return false;

    auto res = std::make_shared<ASTCreateSnapshotQuery>();
    res->if_not_exists = if_not_exists;
    auto table_id = table->as<ASTTableIdentifier>()->getTableId();
    res->database = table_id.database_name;
    res->table = table_id.table_name;
    res->uuid = table_id.uuid;
    if (to_table)
        res->to_table_id = to_table->as<ASTTableIdentifier>()->getTableId();
    res->ttl_in_days = static_cast<Int32>(ttl_in_days);

    node = std::move(res);
    return true;
}

bool ParserCreateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserCreateTableQuery table_p(dt);
    ParserCreateTableAnalyticalMySQLQuery table_mysql_p(dt);
    ParserCreateDatabaseQuery database_p(dt);
    ParserCreateViewQuery view_p(dt);
    ParserCreateDictionaryQuery dictionary_p(dt);
    ParserCreateLiveViewQuery live_view_p(dt);
    ParserCreateCatalogQuery catalog_p(dt);
    ParserCreateSnapshotQuery snapshot_p(dt);

    if (dt.parse_mysql_ddl)
    {
        return table_mysql_p.parse(pos, node, expected)
            || database_p.parse(pos, node, expected)
            || view_p.parse(pos, node, expected)
            || dictionary_p.parse(pos, node, expected)
            || live_view_p.parse(pos, node, expected)
            || catalog_p.parse(pos, node, expected)
            || snapshot_p.parse(pos, node, expected);
    }

    return table_p.parse(pos, node, expected)
        || database_p.parse(pos, node, expected)
        || view_p.parse(pos, node, expected)
        || dictionary_p.parse(pos, node, expected)
        || live_view_p.parse(pos, node, expected)
        || catalog_p.parse(pos, node, expected)
        || snapshot_p.parse(pos, node, expected);
}

bool containsMap(const ASTPtr & ast)
{
    auto * ast_function = ast->as<ASTFunction>();
    if (ast_function)
    {
        String type_name_upper = Poco::toUpper(ast_function->name);
        if (type_name_upper == "MAP")
            return true;

        if (ast_function->arguments)
        {
            bool res = false;
            for (const auto & child: ast_function->arguments->children)
            {
                res |= containsMap(child);
                if (res)
                    break;
            }
            return res;
        }
    }

    return false;
}

}
