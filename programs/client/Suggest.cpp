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

#include "Suggest.h"

#include <Core/Settings.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int DEADLOCK_AVOIDED;
}

void Suggest::load(const ConnectionParameters & connection_parameters, size_t suggestion_limit)
{
    loading_thread = std::thread([connection_parameters, suggestion_limit, this]
    {
        for (size_t retry = 0; retry < 10; ++retry)
        {
            try
            {
                Connection connection(
                    connection_parameters.host,
                    connection_parameters.port,
                    connection_parameters.default_database,
                    connection_parameters.user,
                    connection_parameters.password,
                    "" /* cluster */,
                    "" /* cluster_secret */,
                    "client",
                    connection_parameters.compression,
                    connection_parameters.security);

                loadImpl(connection, connection_parameters.timeouts, suggestion_limit);
            }
            catch (const Exception & e)
            {
                /// Retry when the server said "Client should retry".
                if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
                    continue;

                std::cerr << "Cannot load data for command line suggestions: " << getCurrentExceptionMessage(false, true) << "\n";
            }
            catch (...)
            {
                std::cerr << "Cannot load data for command line suggestions: " << getCurrentExceptionMessage(false, true) << "\n";
            }

            break;
        }

        /// Note that keyword suggestions are available even if we cannot load data from server.

        std::sort(words.begin(), words.end());
        words_no_case = words;
        std::sort(words_no_case.begin(), words_no_case.end(), [](const std::string & str1, const std::string & str2)
        {
            return std::lexicographical_compare(begin(str1), end(str1), begin(str2), end(str2), [](const char char1, const char char2)
            {
                return std::tolower(char1) < std::tolower(char2);
            });
        });

        ready = true;
    });
}

Suggest::Suggest()
{
    /// Keywords may be not up to date with ClickHouse parser.
    words = {"CREATE",       "DATABASE", "IF",     "NOT",       "EXISTS",   "TEMPORARY",   "TABLE",    "ON",          "CLUSTER", "DEFAULT",
             "MATERIALIZED", "ALIAS",    "ENGINE", "AS",        "VIEW",     "POPULATE",    "SETTINGS", "ATTACH",      "DETACH",  "DROP",
             "RENAME",       "TO",       "ALTER",  "ADD",       "MODIFY",   "CLEAR",       "COLUMN",   "AFTER",       "COPY",    "PROJECT",
             "PRIMARY",      "KEY",      "CHECK",  "PARTITION", "PART",     "FREEZE",      "FETCH",    "FROM",        "SHOW",    "INTO",
             "OUTFILE",      "FORMAT",   "TABLES", "DATABASES", "LIKE",     "PROCESSLIST", "CASE",     "WHEN",        "THEN",    "ELSE",
             "END",          "DESCRIBE", "DESC",   "USE",       "SET",      "OPTIMIZE",    "FINAL",    "DEDUPLICATE", "INSERT",  "VALUES",
             "SELECT",       "DISTINCT", "SAMPLE", "ARRAY",     "JOIN",     "GLOBAL",      "LOCAL",    "ANY",         "ALL",     "INNER",
             "LEFT",         "RIGHT",    "FULL",   "OUTER",     "CROSS",    "USING",       "PREWHERE", "WHERE",       "GROUP",   "BY",
             "WITH",         "TOTALS",   "HAVING", "ORDER",     "COLLATE",  "LIMIT",       "UNION",    "AND",         "OR",      "ASC",
             "IN",           "KILL",     "QUERY",  "SYNC",      "ASYNC",    "TEST",        "BETWEEN",  "TRUNCATE",    "USER",    "ROLE",
             "PROFILE",      "QUOTA",    "POLICY", "ROW",       "GRANT",    "REVOKE",      "OPTION",   "ADMIN",       "EXCEPT",  "REPLACE",
             "IDENTIFIED",   "HOST",     "NAME",   "READONLY",  "WRITABLE", "PERMISSIVE",  "FOR",      "RESTRICTIVE", "RANDOMIZED",
             "INTERVAL",     "LIMITS",   "ONLY",   "TRACKING",  "IP",       "REGEXP",      "ILIKE"};
}

void Suggest::loadImpl(Connection & connection, const ConnectionTimeouts & timeouts, size_t suggestion_limit)
{
    /// NOTE: Once you will update the completion list,
    /// do not forget to update 01676_clickhouse_client_autocomplete.sh
/// NOTE: Once you will update the completion list,
    /// do not forget to update 01676_clickhouse_client_autocomplete.sh
    String query;

    auto add_subquery = [&](std::string_view select, std::string_view result_column_name)
    {
        if (!query.empty())
            query += " UNION ALL ";
        query += fmt::format("SELECT * FROM viewIfPermitted({} ELSE null('{} String'))", select, result_column_name);
    };

    auto add_column = [&](std::string_view column_name, std::string_view table_name, bool distinct, std::optional<Int64> limit)
    {
        add_subquery(
            fmt::format(
                "SELECT {}{} FROM system.{}{}",
                (distinct ? "DISTINCT " : ""),
                column_name,
                table_name,
                (limit ? (" LIMIT " + std::to_string(*limit)) : "")),
            column_name);
    };

    add_column("name", "functions", false, {});
    add_column("name", "table_engines", false, {});
    add_column("name", "formats", false, {});
    add_column("name", "table_functions", false, {});
    add_column("name", "data_type_families", false, {});
    add_column("name", "merge_tree_settings", false, {});
    add_column("name", "settings", false, {});
    add_column("cluster", "clusters", false, {});
    add_column("macro", "macros", false, {});
    add_column("policy_name", "storage_policies", false, {});

    add_subquery("SELECT concat(func.name, comb.name) AS x FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate", "x");

    /// The user may disable loading of databases, tables, columns by setting suggestion_limit to zero.
    if (suggestion_limit > 0)
    {
        add_column("name", "databases", false, suggestion_limit);
        add_column("name", "tables", true, suggestion_limit);
        add_column("name", "dictionaries", true, suggestion_limit);
        add_column("name", "columns", true, suggestion_limit);
    }

    query = "SELECT DISTINCT arrayJoin(extractAll(name, '[\\\\w_]{2,}')) AS res FROM (" + query + ") WHERE notEmpty(arrayJoin(extractAll(name, '[\\\\w_]{2,}')))";
    fetch(connection, timeouts, query);
}

void Suggest::fetch(Connection & connection, const ConnectionTimeouts & timeouts, const std::string & query)
{
    connection.sendQuery(timeouts, query, "" /* query_id */, QueryProcessingStage::Complete);

    while (true)
    {
        Packet packet = connection.receivePacket();
        switch (packet.type)
        {
            case Protocol::Server::Data:
                fillWordsFromBlock(packet.block);
                continue;

            case Protocol::Server::Progress:
                continue;
            case Protocol::Server::ProfileInfo:
                continue;
            case Protocol::Server::QueryMetrics:
                continue;
            case Protocol::Server::Totals:
                continue;
            case Protocol::Server::Extremes:
                continue;
            case Protocol::Server::Log:
                continue;

            case Protocol::Server::Exception:
                packet.exception->rethrow();
                return;

            case Protocol::Server::EndOfStream:
                return;

            default:
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}",
                    packet.type, connection.getDescription());
        }
    }
}

void Suggest::fillWordsFromBlock(const Block & block)
{
    if (!block)
        return;

    if (block.columns() != 1)
        throw Exception("Wrong number of columns received for query to read words for suggestion", ErrorCodes::LOGICAL_ERROR);

    const ColumnString & column = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);

    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
        words.emplace_back(column.getDataAt(i).toString());
}

}
