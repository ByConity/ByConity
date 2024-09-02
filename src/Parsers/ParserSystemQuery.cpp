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

#include <Interpreters/StorageID.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ParserPartition.h>

namespace ErrorCodes
{
}


namespace DB
{

static bool parseQueryWithOnClusterAndMaybeTable(std::shared_ptr<ASTSystemQuery> & res, IParser::Pos & pos,
                                                 Expected & expected, bool require_table, bool allow_string_literal)
{
    /// Better form for user: SYSTEM <ACTION> table ON CLUSTER cluster
    /// Query rewritten form + form while executing on cluster: SYSTEM <ACTION> ON CLUSTER cluster table
    /// Need to support both
    String cluster;
    bool parsed_on_cluster = false;

    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;
        parsed_on_cluster = true;
    }

    bool parsed_table = false;
    if (allow_string_literal)
    {
        ASTPtr ast;
        if (ParserStringLiteral{}.parse(pos, ast, expected))
        {
            res->database = {};
            res->table = ast->as<ASTLiteral &>().value.safeGet<String>();
            parsed_table = true;
        }
    }

    if (!parsed_table)
        parsed_table = parseDatabaseAndTableName(pos, expected, res->database, res->table);

    if (!parsed_table && require_table)
            return false;

    if (!parsed_on_cluster && ParserKeyword{"ON"}.ignore(pos, expected))
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;

    res->cluster = cluster;
    return true;
}

bool ParserSystemQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SYSTEM"}.ignore(pos, expected))
        return false;

    using Type = ASTSystemQuery::Type;

    auto res = std::make_shared<ASTSystemQuery>();

    bool found = false;
    for (int i = static_cast<int>(Type::UNKNOWN) + 1; i < static_cast<int>(Type::END); ++i)
    {
        Type t = static_cast<Type>(i);
        if (ParserKeyword{ASTSystemQuery::typeToString(t)}.ignore(pos, expected))
        {
            res->type = t;
            found = true;
        }
    }

    if (!found)
        return false;

    // Parse integer
    auto parse_uint = [](IParser::Pos & pos_, Expected & expected_, UInt64 & res_)
    {
        ASTPtr ast;
        if (!ParserUnsignedInteger().parse(pos_, ast, expected_)) return false;
        res_ = safeGet<UInt64>(ast->as<ASTLiteral>()->value);
        return true;
    };
    ParserPartition parser_partition;
    switch (res->type)
    {
        case Type::RELOAD_DICTIONARY:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ true))
                return false;
            break;
        }
        case Type::RELOAD_MODEL:
        {
            String cluster_str;
            if (ParserKeyword{"ON"}.ignore(pos, expected))
            {
                if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                    return false;
            }
            res->cluster = cluster_str;
            ASTPtr ast;
            if (ParserStringLiteral{}.parse(pos, ast, expected))
            {
                res->target_model = ast->as<ASTLiteral &>().value.safeGet<String>();
            }
            else
            {
                ParserIdentifier model_parser;
                ASTPtr model;
                String target_model;

                if (!model_parser.parse(pos, model, expected))
                    return false;

                if (!tryGetIdentifierNameInto(model, res->target_model))
                    return false;
            }

            break;
        }
        case Type::DROP_REPLICA:
        {
            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            res->replica = ast->as<ASTLiteral &>().value.safeGet<String>();
            if (ParserKeyword{"FROM"}.ignore(pos, expected))
            {
                // way 1. parse replica database
                // way 2. parse replica tables
                // way 3. parse replica zkpath
                if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
                {
                    ParserIdentifier database_parser;
                    ASTPtr database;
                    if (!database_parser.parse(pos, database, expected))
                        return false;
                    tryRewriteCnchDatabaseName(database, pos.getContext());
                    tryGetIdentifierNameInto(database, res->database);
                }
                else if (ParserKeyword{"TABLE"}.ignore(pos, expected))
                {
                    parseDatabaseAndTableName(pos, expected, res->database, res->table);
                }
                else if (ParserKeyword{"ZKPATH"}.ignore(pos, expected))
                {
                    ASTPtr path_ast;
                    if (!ParserStringLiteral{}.parse(pos, path_ast, expected))
                        return false;
                    String zk_path = path_ast->as<ASTLiteral &>().value.safeGet<String>();
                    if (!zk_path.empty() && zk_path[zk_path.size() - 1] == '/')
                        zk_path.pop_back();
                    res->replica_zk_path = zk_path;
                }
                else
                    return false;
            }
            else
                res->is_drop_whole_replica = true;

            break;
        }

        case Type::RECALCULATE_METRICS:
        case Type::RESTART_REPLICA:
        case Type::SYNC_REPLICA:
            if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                return false;
            break;

        case Type::RESTART_DISK:
        {
            ASTPtr ast;
            if (ParserIdentifier{}.parse(pos, ast, expected))
                res->disk = ast->as<ASTIdentifier &>().name();
            else
                return false;

            break;
        }

        /// FLUSH DISTRIBUTED requires table
        /// START/STOP DISTRIBUTED SENDS does not require table
        case Type::STOP_DISTRIBUTED_SENDS:
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ false, /* allow_string_literal = */ false))
                return false;
            break;
        }

        case Type::FLUSH_DISTRIBUTED:
        case Type::RESTORE_REPLICA:
        {
            if (!parseQueryWithOnClusterAndMaybeTable(res, pos, expected, /* require table = */ true, /* allow_string_literal = */ false))
                return false;
            break;
        }

        case Type::STOP_MERGES:
        case Type::START_MERGES:
        case Type::REMOVE_MERGES:
        {
            String storage_policy_str;
            String volume_str;

            if (ParserKeyword{"ON VOLUME"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (ParserIdentifier{}.parse(pos, ast, expected))
                    storage_policy_str = ast->as<ASTIdentifier &>().name();
                else
                    return false;

                if (!ParserToken{TokenType::Dot}.ignore(pos, expected))
                    return false;

                if (ParserIdentifier{}.parse(pos, ast, expected))
                    volume_str = ast->as<ASTIdentifier &>().name();
                else
                    return false;
            }
            res->storage_policy = storage_policy_str;
            res->volume = volume_str;
            if (res->volume.empty() && res->storage_policy.empty())
                parseDatabaseAndTableName(pos, expected, res->database, res->table);
            break;
        }

        case Type::GC:
        {
            if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                return false;
            if (ParserKeyword{"PARTITION"}.ignore(pos, expected) && !parser_partition.parse(pos, res->partition, expected))
                return false;
            break;
        }

        case Type::START_GC:
        case Type::STOP_GC:
        case Type::FORCE_GC:
        case Type::DROP_CNCH_META_CACHE:
        case Type::DROP_CNCH_PART_CACHE:
        case Type::DROP_CNCH_DELETE_BITMAP_CACHE:
        case Type::STOP_TTL_MERGES:
        case Type::START_TTL_MERGES:
        case Type::STOP_MOVES:
        case Type::START_MOVES:
        case Type::STOP_FETCHES:
        case Type::START_FETCHES:
        case Type::STOP_REPLICATED_SENDS:
        case Type::START_REPLICATED_SENDS:
        case Type::STOP_REPLICATION_QUEUES:
        case Type::START_REPLICATION_QUEUES:
        case Type::START_CONSUME:
        case Type::STOP_CONSUME:
        case Type::DROP_CONSUME:
        case Type::RESTART_CONSUME:
        case Type::RESYNC_MATERIALIZEDMYSQL_TABLE:
        case Type::DROP_CHECKSUMS_CACHE:
        case Type::SYNC_DEDUP_WORKER:
        case Type::SYNC_REPAIR_TASK:
        case Type::START_DEDUP_WORKER:
        case Type::STOP_DEDUP_WORKER:
        case Type::START_CLUSTER:
        case Type::STOP_CLUSTER:
        case Type::FLUSH_CNCH_LOG:
        case Type::STOP_CNCH_LOG:
        case Type::RESUME_CNCH_LOG:
        case Type::MANIFEST_CHECKPOINT:
            parseDatabaseAndTableName(pos, expected, res->database, res->table);
            break;

        case Type::SUSPEND:
        {
            ASTPtr seconds;
            if (!(ParserKeyword{"FOR"}.ignore(pos, expected)
                && ParserUnsignedInteger().parse(pos, seconds, expected)
                && ParserKeyword{"SECOND"}.ignore(pos, expected)))   /// SECOND, not SECONDS to be consistent with INTERVAL parsing in SQL
            {
                return false;
            }

            res->seconds = seconds->as<ASTLiteral>()->value.get<UInt64>();
            break;
        }

        case Type::FETCH_PARTS:
        {
            if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                return false;
            if (!ParserStringLiteral().parse(pos, res->target_path, expected))
                return false;
            break;
        }

        case Type::METASTORE:
        {
            if (ParserKeyword{"SYNC"}.ignore(pos, expected))
            {
                res->meta_ops.operation = MetastoreOperation::SYNC;
                if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                    return false;
            }
            else if (ParserKeyword{"DROP"}.ignore(pos, expected))
            {
                if (ParserKeyword{"BY KEY"}.ignore(pos, expected))
                {
                    res->meta_ops.operation = MetastoreOperation::DROP_BY_KEY;
                    ASTPtr ast_literal;
                    if (!ParserStringLiteral{}.parse(pos, ast_literal, expected))
                        return false;
                    res->meta_ops.drop_key= ast_literal->as<ASTLiteral &>().value.safeGet<String>();
                }
                else
                    res->meta_ops.operation = MetastoreOperation::DROP_ALL_KEY;

                if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                    return false;
            }
            else if (ParserKeyword{"STOP AUTO SYNC"}.ignore(pos, expected))
            {
                res->meta_ops.operation = MetastoreOperation::STOP_AUTO_SYNC;
            }
            else if (ParserKeyword{"START AUTO SYNC"}.ignore(pos, expected))
            {
                res->meta_ops.operation = MetastoreOperation::START_AUTO_SYNC;
            }
            else
                return false;

            break;
        }

        case Type::DEDUP_WITH_HIGH_PRIORITY:
        {
            if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                return false;
            if (!ParserKeyword{"PARTITION"}.ignore(pos, expected))
                return false;
            if (!parser_partition.parse(pos, res->partition, expected))
                return false;
            break;
        }

        case Type::DEDUP:
        {
            if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                return false;
            if (ParserKeyword{"PARTITION"}.ignore(pos, expected) && !parser_partition.parse(pos, res->partition, expected))
                return false;
            if (ParserKeyword{"BUCKET"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserUnsignedInteger().parse(pos, ast, expected))
                    return false;
                res->specify_bucket = true;
                res->bucket_number = safeGet<UInt64>(ast->as<ASTLiteral>()->value);
            }
            if (!ParserKeyword{"FOR REPAIR"}.ignore(pos, expected))
                return false;
            break;
        }

        case Type::RESET_CONSUME_OFFSET:
        {
            if (!parseIdentifierOrStringLiteral(pos, expected, res->string_data))
                return false;
            break;
        }

        case Type::CLEAN_TRASH_TABLE:
        {
            ASTPtr table;
            ParserCompoundIdentifier name_p(true);
            if (!name_p.parse(pos, table, expected))
                return false;
            auto table_id = table->as<ASTTableIdentifier>()->getTableId();
            res->database = table_id.database_name;
            res->table = table_id.table_name;
            res->table_uuid = table_id.uuid;
            break;
        }

        case Type::CLEAN_TRANSACTION:
        {
            if (!parse_uint(pos, expected, res->txn_id))
                return false;
            break;
        }

        case Type::START_MATERIALIZEDMYSQL:
        case Type::STOP_MATERIALIZEDMYSQL:
        {
            if (!parseIdentifierOrStringLiteral(pos, expected, res->database))
                return false;
            break;
        }

        case Type::LOCK_MEMORY_LOCK:
        {
            parseDatabaseAndTableName(pos, expected, res->database, res->table);
            if (ParserKeyword{"PARTITION"}.ignore(pos, expected) && !parser_partition.parse(pos, res->partition, expected))
                return false;

            ASTPtr seconds;
            if (!(ParserKeyword{"FOR"}.ignore(pos, expected)
                && ParserUnsignedInteger().parse(pos, seconds, expected)
                && ParserKeyword{"SECOND"}.ignore(pos, expected)))   /// SECOND, not SECONDS to be consistent with INTERVAL parsing in SQL
            {
                return false;
            }

            res->seconds = seconds->as<ASTLiteral>()->value.get<UInt64>();

            if (ParserKeyword{"DOMAIN"}.ignore(pos, expected))
                res->string_data = "DOMAIN";

            break;
        }

        case Type::START_VIEW:
        case Type::STOP_VIEW:
        case Type::DROP_VIEW_META:
            if (!parseDatabaseAndTableName(pos, expected, res->database, res->table))
                return false;
            break;

        case Type::RELEASE_MEMORY_LOCK:
        {
            if (ParserKeyword{"OF"}.ignore(pos, expected))
            {
                if (!ParserKeyword{"TXN"}.ignore(pos, expected))
                    return false;
                if (!parse_uint(pos, expected, res->txn_id))
                    return false;
                res->specify_txn = true;
            }
            else
                parseDatabaseAndTableName(pos, expected, res->database, res->table);
            break;
        }

        case Type::DROP_SCHEMA_CACHE:
        {
            if (ParserKeyword{"FOR"}.ignore(pos, expected))
            {
                if (ParserKeyword{"S3"}.ignore(pos, expected))
                    res->schema_cache_storage = "S3";
                else
                    return false;
            }
            break;
        }

        default:
            /// There are no [db.table] after COMMAND NAME
            break;
    }

    if (res->predicate)
        res->children.push_back(res->predicate);


    node = std::move(res);
    return true;
}

}
