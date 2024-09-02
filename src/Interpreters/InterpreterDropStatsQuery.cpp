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

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropStatsQuery.h>
#include <Parsers/ASTStatsQuery.h>
#include <Statistics/ASTHelpers.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CatalogAdaptorProxy.h>
#include <Statistics/DropHelper.h>
#include <Statistics/OptimizerStatisticsClient.h>
#include <Statistics/StatsTableBasic.h>
#include <Statistics/ASTHelpers.h>

namespace DB
{
using namespace Statistics;
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
}

BlockIO InterpreterDropStatsQuery::execute()
{
    auto context = getContext();
    auto query = query_ptr->as<const ASTDropStatsQuery>();
    auto catalog = Statistics::createCatalogAdaptor(context);

    auto cache_policy = query->cache_policy;

    if (cache_policy == StatisticsCachePolicy::Default)
    {
        // use context settings
        cache_policy = context->getSettingsRef().statistics_cache_policy;
    }

    // when enable_memory_catalog is true, we won't use cache
    if (catalog->getSettingsRef().enable_memory_catalog)
    {
        if (cache_policy != StatisticsCachePolicy::Default)
            throw Exception("memory catalog don't support cache policy", ErrorCodes::BAD_ARGUMENTS);
    }

    if (query->table == "__reset")
    {
        throw Exception("unsupported", ErrorCodes::NOT_IMPLEMENTED);
    }

    catalog->checkHealth(/*is_write=*/true);

    auto proxy = Statistics::createCatalogAdaptorProxy(catalog, cache_policy);
    auto db = context->resolveDatabase(query->database);

    if (!DatabaseCatalog::instance().isDatabaseExist(db, context))
    {
        auto msg = fmt::format(FMT_STRING("Unknown database ({})"), db);
        throw Exception(msg, ErrorCodes::UNKNOWN_DATABASE);
    }

    auto tables = getTablesFromAST(context, query);


    if (tables.size() == 1 && !query->columns.empty())
    {
        auto table = tables[0];
        dropStatsColumns(context, table, query->columns, cache_policy, true);
    }
    else
    {
        for (auto table : tables)
        {
            dropStatsTable(context, table, cache_policy, true);
        }
    }

    return {};
}
}
