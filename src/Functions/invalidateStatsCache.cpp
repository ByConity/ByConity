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

#if 0
#include <Functions/FunctionFactory.h>
#include <Functions/invalidateStatsCache.h>
#include <Statistics/CatalogAdaptor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

REGISTER_FUNCTION(InvalidateStatsCache)
{
    factory.registerFunction<FunctionInvalidateStatsCache>();
}

ColumnPtr FunctionInvalidateStatsCache::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    std::vector<String> identifier_names;
    for (const auto & pr : arguments)
    {
        const IColumn * raw_col = pr.column.get();
        if (!isColumnConst(*raw_col))
            throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);
        const auto * col = dynamic_cast<const ColumnConst *>(raw_col);
        if (!col)
            throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        auto identifier_name = col->getField().get<String>();
        identifier_names.emplace_back(std::move(identifier_name));
    }

    // TODO support variadic, i.e. invalidate full database/cluster
    if (identifier_names.size() != 2)
        throw Exception("Function " + getName() + " requires two arguments", ErrorCodes::BAD_ARGUMENTS);

    if (identifier_names[0].empty() && identifier_names[1] == "__reset")
    {
        auto catalog = Statistics::createConstCatalogAdaptor(context);
        catalog->invalidateAllServerStatsCache();
        return result_type->createColumnConst(input_rows_count, 0);
    }

    auto catalog = Statistics::createConstCatalogAdaptor(context);
    auto table_identifier_opt = catalog->getTableIdByName(identifier_names[0], identifier_names[1]);

    if (!table_identifier_opt.has_value())
    {
        // DO NOTHING
        LOG_INFO(getLogger("invalidateStatsCache"), "Table " + identifier_names[0] + "." + identifier_names[1] + " not found, skip cache clear");
    }
    else
    {
        // a single function will clear one server
        catalog->invalidateServerStatsCache(*table_identifier_opt);
    }

    return result_type->createColumnConst(input_rows_count, 0);
}
}
#endif
