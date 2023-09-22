/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTPartToolKit.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserPartToolkitQuery.h>
#include "Parsers/parseIdentifierOrStringLiteral.h"

namespace DB
{

bool ParserPWStorage::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_engine("ENGINE");
    ParserToken s_eq(TokenType::Equals);
    ParserKeyword s_partition_by("PARTITION BY");
    ParserKeyword s_primary_key("PRIMARY KEY");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_unique_key("UNIQUE KEY");
    ParserKeyword s_cluster_by("CLUSTER BY");
    ParserKeyword s_sample_by("SAMPLE BY");
    ParserKeyword s_ttl("TTL");

    ParserIdentifierWithOptionalParameters ident_with_optional_params_p(dt);
    ParserExpression expression_p(dt);
    ParserClusterByElement cluster_p;

    ASTPtr engine;
    ASTPtr partition_by;
    ASTPtr primary_key;
    ASTPtr order_by;
    ASTPtr unique_key;
    ASTPtr cluster_by;
    ASTPtr sample_by;
    ASTPtr ttl_table;

    bool has_explicit_engine = s_engine.checkWithoutMoving(pos, expected);
    if (has_explicit_engine)
    {
        s_engine.ignore(pos, expected);
        s_eq.ignore(pos, expected);
        if (!ident_with_optional_params_p.parse(pos, engine, expected))
            return false;
    }

    while (true)
    {
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

        if (!cluster_by && s_cluster_by.ignore(pos, expected))
        {
            if (!cluster_p.parse(pos, cluster_by, expected))
                return false;

            continue;
        }

        if (!sample_by && s_sample_by.ignore(pos, expected))
        {
            if (!expression_p.parse(pos, sample_by, expected))
                return false;
            continue;
        }

        /// parse TTL as well to prevent parse error
        if (!ttl_table && s_ttl.ignore(pos, expected))
        {
            if (!expression_p.parse(pos, ttl_table, expected))
                return false;
            continue;
        }

        break;
    }

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
    storage->set(storage->unique_key, unique_key);
    storage->set(storage->cluster_by, cluster_by);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->ttl_table, ttl_table);

    if (has_explicit_engine)
        storage->set(storage->engine, engine);
    else
    {
        /// Mock a CloudMergeTree engine
        std::shared_ptr<ASTFunction> implicit_engine = std::make_shared<ASTFunction>();
        implicit_engine->name = "CloudMergeTree";
        implicit_engine->arguments = std::make_shared<ASTExpressionList>();
        implicit_engine->arguments->children.emplace_back(std::make_shared<ASTIdentifier>("default"));
        implicit_engine->arguments->children.emplace_back(std::make_shared<ASTIdentifier>("tmp"));
        implicit_engine->no_empty_args = true;
        storage->set(storage->engine, implicit_engine);
    }

    node = storage;
    return true;
}

bool ParserPartToolkitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_load("LOAD");
    ParserKeyword s_clean("CLEAN");
    ParserKeyword s_file("FILE");
    ParserKeyword s_merge("MERGE PARTS");
    ParserKeyword s_converter("CONVERT PARTS FROM");
    ParserKeyword s_to("TO");
    ParserKeyword s_as_table("AS TABLE");
    ParserKeyword s_location("LOCATION");
    ParserKeyword s_setting("SETTINGS");
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserPWStorage storage_p;
    ParserCompoundIdentifier table_name_p(true);
    ParserTablePropertiesDeclarationList table_properties_p(dt);
    ParserIdentifier name_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ParserStringLiteral string_literal_parser;

    ASTPtr table;
    ASTPtr columns_list;
    ASTPtr storage;
    ASTPtr source_path;
    ASTPtr data_format;
    ASTPtr target_path;
    ASTPtr settings;

    PartToolType type;

    if (s_load.ignore(pos, expected))
    {
        /// parse input format
        if (!name_p.parse(pos, data_format, expected))
            return false;

        if (!s_file.ignore(pos, expected))
            return false;

        type = PartToolType::WRITER;
    }
    else if (s_merge.ignore(pos, expected))
    {
        /// no more details
        type = PartToolType::MERGER;
    }
    else if (s_converter.ignore(pos, expected))
    {
        if (!string_literal_parser.parse(pos, source_path, expected))
            return false;

        if (!s_to.ignore(pos, expected))
            return false;

        /// parse output format
        if (!name_p.parse(pos, data_format, expected))
            return false;

        type = PartToolType::CONVERTER;
    }
    else if (s_clean.ignore(pos, expected))
    {
        // CLEAN
        // `clean s3 task all '<task_id>' settings s3_config='./s3.conf'`
        type = PartToolType::NOTYPE;

        auto clean_info = std::make_shared<S3CleanTaskInfo>();
        ParserKeyword s_s3("S3");
        ParserKeyword s_task("TASK");
        ParserKeyword s_meta("META");
        ParserKeyword s_data("DATA");
        ParserKeyword s_all("ALL");

        ParserSetQuery settings_p_local(/* parse_only_internals_ = */ true);

        if (!s_s3.ignore(pos, expected))
            return false;

        if (!s_task.ignore(pos, expected))
            return false;

        if (s_meta.ignore(pos, expected))
        {
            clean_info->type = S3CleanTaskInfo::CleanType::META;
        }
        else if (s_all.ignore(pos, expected))
        {
            clean_info->type = S3CleanTaskInfo::CleanType::ALL;
        }
        else if (s_data.ignore(pos, expected))
        {
            clean_info->type = S3CleanTaskInfo::CleanType::DATA;
        }
        else
            return false;

        if (!parseIdentifierOrStringLiteral(pos, expected, clean_info->task_id))
            return false;

        if (s_setting.ignore(pos, expected))
        {
            if (!settings_p_local.parse(pos, settings, expected))
                return false;
        }

        auto part_toolkit_ast = std::make_shared<ASTPartToolKit>();
        node = part_toolkit_ast;
        part_toolkit_ast->type = type;
        part_toolkit_ast->s3_clean_task_info = clean_info;
        part_toolkit_ast->settings = settings;

        return true;
    }
    else
        return false;


    if (type != PartToolType::CONVERTER && !string_literal_parser.parse(pos, source_path, expected))
        return false;

    if (!s_as_table.ignore(pos, expected))
        return false;

    /// table name is optional, just ignore it.
    table_name_p.parse(pos, table, expected);
    tryRewriteCnchDatabaseName(table, pos.getContext());

    if (!s_lparen.ignore(pos, expected))
        return false;

    if (!table_properties_p.parse(pos, columns_list, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    if (!storage_p.parse(pos, storage, expected))
        return false;

    if (s_location.ignore(pos, expected))
    {
        if (!string_literal_parser.parse(pos, target_path, expected))
            return false;
    }
    else
    {
        return false;
    }

    if (s_setting.ignore(pos, expected))
    {
        if (!settings_p.parse(pos, settings, expected))
            return false;
    }

    auto create_query = std::make_shared<ASTCreateQuery>();
    StorageID table_id = StorageID::createEmpty();
    if (table)
        table_id = table->as<ASTTableIdentifier>()->getTableId();
    else
        table_id = StorageID("default", "tmp");

    create_query->database = table_id.database_name;
    create_query->table = table_id.table_name;
    create_query->uuid = table_id.uuid;
    create_query->set(create_query->columns_list, columns_list);
    create_query->set(create_query->storage, storage);
    /// if uuid not specified, generate one for current table.
    if (create_query->uuid == UUIDHelpers::Nil)
        create_query->uuid = UUIDHelpers::generateV4();

    auto part_toolkit_ast = std::make_shared<ASTPartToolKit>();
    node = part_toolkit_ast;

    part_toolkit_ast->type = type;
    part_toolkit_ast->source_path = source_path;
    part_toolkit_ast->create_query = create_query;
    part_toolkit_ast->target_path = target_path;
    part_toolkit_ast->data_format = data_format;
    part_toolkit_ast->settings = settings;

    return true;
}

}
