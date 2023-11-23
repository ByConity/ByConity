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

#include <QueryPlan/planning_common.h>

namespace DB
{

void putIdentities(const NamesAndTypes & columns, Assignments & assignments, NameToType & types)
{
    for (const auto & col: columns)
    {
        assignments.emplace_back(col.name, toSymbolRef(col.name));
        types[col.name] = col.type;
    }
}

void mapFieldSymbolInfos(FieldSymbolInfos & symbol_infos, const NameToNameMap & name_mapping, bool map_sub_column)
{
    for (auto & symbol_info : symbol_infos)
    {
        if (auto it = name_mapping.find(symbol_info.primary_symbol); it != name_mapping.end())
            symbol_info.primary_symbol = it->second;

        if (map_sub_column)
        {
            for (auto & sub_symbol_info : symbol_info.sub_column_symbols)
                if (auto it = name_mapping.find(sub_symbol_info.second); it != name_mapping.end())
                    sub_symbol_info.second = it->second;
        }
    }
}
}
