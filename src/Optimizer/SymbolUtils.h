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

#pragma once
#include <Core/Field.h>
#include <set>

namespace DB
{
class SymbolUtils
{
public:
    static bool contains(const std::vector<String> & symbols, const String & symbol);
    static bool containsAll(const std::set<String> & left_symbols, const std::set<String> & right_symbols);
    static bool containsAll(const std::vector<String> & left_symbols, const std::set<String> & right_symbols);

    template <typename K, typename... TArgs, template<typename...> class T>
    static bool containsAll(const std::unordered_set<K> & left_symbols, const T<K, TArgs...> & right_symbols)
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
};

}
