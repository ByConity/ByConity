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

#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTableFunction.h>
#include <Access/AccessFlags.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event TableFunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name,
                                   ColumnsDescription cached_columns) const
{
    if (isPreviledgedFunction() && context->shouldBlockPrivilegedOperations())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tenant cannot execute this function {} for security reason.", getName());

    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);
    AccessFlags required_access =  StorageFactory::instance().getSourceAccessType(getStorageTypeName());
    String function_name = getName();
    if ((function_name != "null") && (function_name != "view") && (function_name != "viewIfPermitted"))
        required_access |= AccessType::CREATE_TEMPORARY_TABLE;
    context->checkAccess(required_access);

    if (cached_columns.empty())
        return executeImpl(ast_function, context, table_name, std::move(cached_columns));

    /// We have table structure, so it's CREATE AS table_function().
    /// We should use global context here because there will be no query context on server startup
    /// and because storage lifetime is bigger than query context lifetime.
    auto global_context = context->getGlobalContext();
    if (hasStaticStructure() && cached_columns == getActualTableStructure(context))
        return executeImpl(ast_function, global_context, table_name, std::move(cached_columns));

    auto this_table_function = shared_from_this();
    auto get_storage = [=]() -> StoragePtr
    {
        return this_table_function->executeImpl(ast_function, global_context, table_name, cached_columns);
    };

    /// It will request actual table structure and create underlying storage lazily
    return std::make_shared<StorageTableFunctionProxy>(StorageID(getDatabaseName(), table_name), std::move(get_storage),
                                                       std::move(cached_columns), needStructureConversion());
}

/// The Cartesian product of two sets of rows, the result is written in place of the first argument
static void append(std::vector<String> & to, const std::vector<String> & what, size_t max_addresses)
{
    if (what.empty())
        return;

    if (to.empty())
    {
        to = what;
        return;
    }

    if (what.size() * to.size() > max_addresses)
        throw Exception("Table function 'remote': first argument generates too many result addresses",
                        ErrorCodes::BAD_ARGUMENTS);
    std::vector<String> res;
    for (size_t i = 0; i < to.size(); ++i)
        for (size_t j = 0; j < what.size(); ++j)
            res.push_back(to[i] + what[j]);

    to.swap(res);
}

/// Parse number from substring
static bool parseNumber(const String & description, size_t l, size_t r, size_t & res)
{
    res = 0;
    for (size_t pos = l; pos < r; pos ++)
    {
        if (!isNumericASCII(description[pos]))
            return false;
        res = res * 10 + description[pos] - '0';
        if (res > 1e15)
            return false;
    }
    return true;
}

/* Parse a string that generates shards and replicas. Separator - one of two characters | or ,
 *  depending on whether shards or replicas are generated.
 * For example:
 * host1,host2,...      - generates set of shards from host1, host2, ...
 * host1|host2|...      - generates set of replicas from host1, host2, ...
 * abc{8..10}def        - generates set of shards abc8def, abc9def, abc10def.
 * abc{08..10}def       - generates set of shards abc08def, abc09def, abc10def.
 * abc{x,yy,z}def       - generates set of shards abcxdef, abcyydef, abczdef.
 * abc{x|yy|z} def      - generates set of replicas abcxdef, abcyydef, abczdef.
 * abc{1..9}de{f,g,h}   - is a direct product, 27 shards.
 * abc{1..9}de{0|1}     - is a direct product, 9 shards, in each 2 replicas.
 */
std::vector<String> parseDescription(const String & description, size_t l, size_t r, char separator, size_t max_addresses)
{
    std::vector<String> res;
    std::vector<String> cur;

    /// An empty substring means a set of an empty string
    if (l >= r)
    {
        res.push_back("");
        return res;
    }

    for (size_t i = l; i < r; ++i)
    {
        /// Either the numeric interval (8..10) or equivalent expression in brackets
        if (description[i] == '{')
        {
            int cnt = 1;
            int last_dot = -1; /// The rightmost pair of points, remember the index of the right of the two
            size_t m;
            std::vector<String> buffer;
            bool have_splitter = false;

            /// Look for the corresponding closing bracket
            for (m = i + 1; m < r; ++m)
            {
                if (description[m] == '{') ++cnt;
                if (description[m] == '}') --cnt;
                if (description[m] == '.' && description[m-1] == '.') last_dot = m;
                if (description[m] == separator) have_splitter = true;
                if (cnt == 0) break;
            }
            if (cnt != 0)
                throw Exception("Table function 'remote': incorrect brace sequence in first argument",
                                ErrorCodes::BAD_ARGUMENTS);
            /// The presence of a dot - numeric interval
            if (last_dot != -1)
            {
                size_t left, right;
                if (description[last_dot - 1] != '.')
                    throw Exception("Table function 'remote': incorrect argument in braces (only one dot): " + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (!parseNumber(description, i + 1, last_dot - 1, left))
                    throw Exception("Table function 'remote': incorrect argument in braces (Incorrect left number): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (!parseNumber(description, last_dot + 1, m, right))
                    throw Exception("Table function 'remote': incorrect argument in braces (Incorrect right number): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (left > right)
                    throw Exception("Table function 'remote': incorrect argument in braces (left number is greater then right): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (right - left + 1 >  max_addresses)
                    throw Exception("Table function 'remote': first argument generates too many result addresses",
                        ErrorCodes::BAD_ARGUMENTS);
                bool add_leading_zeroes = false;
                size_t len = last_dot - 1 - (i + 1);
                 /// If the left and right borders have equal numbers, then you must add leading zeros.
                if (last_dot - 1 - (i + 1) == m - (last_dot + 1))
                    add_leading_zeroes = true;
                for (size_t id = left; id <= right; ++id)
                {
                    String str = toString<UInt64>(id);
                    if (add_leading_zeroes)
                    {
                        while (str.size() < len)
                            str = "0" + str;
                    }
                    buffer.push_back(str);
                }
            }
            else if (have_splitter) /// If there is a current delimiter inside, then generate a set of resulting rows
                buffer = parseDescription(description, i + 1, m, separator, max_addresses);
            else                     /// Otherwise just copy, spawn will occur when you call with the correct delimiter
                buffer.push_back(description.substr(i, m - i + 1));
            /// Add all possible received extensions to the current set of lines
            append(cur, buffer, max_addresses);
            i = m;
        }
        else if (description[i] == separator)
        {
            /// If the delimiter, then add found rows
            res.insert(res.end(), cur.begin(), cur.end());
            cur.clear();
        }
        else
        {
            /// Otherwise, simply append the character to current lines
            std::vector<String> buffer;
            buffer.push_back(description.substr(i, 1));
            append(cur, buffer, max_addresses);
        }
    }

    res.insert(res.end(), cur.begin(), cur.end());
    if (res.size() > max_addresses)
        throw Exception("Table function 'remote': first argument generates too many result addresses",
            ErrorCodes::BAD_ARGUMENTS);

    return res;
}

}
