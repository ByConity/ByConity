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

#include <algorithm>
#include <Optimizer/SymbolUtils.h>

namespace DB
{
bool SymbolUtils::contains(const std::vector<String> & symbols, const String & symbol)
{
    return std::find(symbols.begin(), symbols.end(), symbol) != symbols.end();
}

bool SymbolUtils::containsAll(const std::set<String> & left_symbols, const std::set<String> & right_symbols)
{
    for (const auto & symbol : right_symbols)
    {
        if (!left_symbols.contains(symbol))
        {
            return false;
        }
    }
    return true;
}

bool SymbolUtils::containsAll(const std::vector<String> & left_symbols, const std::set<String> & right_symbols)
{
    for (const auto & symbol : right_symbols)
    {
        if (!SymbolUtils::contains(left_symbols, symbol))
        {
            return false;
        }
    }
    return true;
}

}
