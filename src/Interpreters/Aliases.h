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

#pragma once

#include <common/types.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_map>

namespace DB
{

class Aliases
{
public:
    ASTPtr & operator[](const String & name)
    {
        return getImpl(name, true);
    }
    const ASTPtr & operator[](const String & name) const
    {
        return const_cast<Aliases *>(this)->operator[](name);
    }
    ASTPtr & at(const String & name)
    {
        return getImpl(name, false);
    }
    const ASTPtr & at(const String & name) const
    {
        return const_cast<Aliases *>(this)->at(name);
    }

    auto count(const String & name) const
    {
        checkSingleton(name);
        return names_to_asts.count(name);
    }
    auto contains(const String & name) const
    {
        checkSingleton(name);
        return names_to_asts.contains(name);
    }
    auto find(const String & name)
    {
        checkSingleton(name);
        return names_to_asts.find(name);
    }
    auto find(const String & name) const
    {
        checkSingleton(name);
        return names_to_asts.find(name);
    }
    auto begin()
    {
        checkSingleton();
        return names_to_asts.begin();
    }
    auto begin() const
    {
        checkSingleton();
        return names_to_asts.begin();
    }
    auto end()
    {
        return names_to_asts.end();
    }
    auto end() const
    {
        return names_to_asts.end();
    }
    auto empty() const
    {
        return names_to_asts.empty();
    }
    template <class... Args>
    auto emplace(Args &&... args)
    {
        return names_to_asts.emplace(std::forward<Args>(args)...);
    }

private:
    void checkSingleton() const;
    void checkSingleton(const String & name) const;
    ASTPtr & getImpl(const String & name, bool insert_if_not_exists);

    std::unordered_multimap<String, ASTPtr> names_to_asts;
};
}
