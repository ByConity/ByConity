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

#include <Core/Types.h>
#include <Parsers/formatAST.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <boost/hana.hpp>

#include <functional>
#include <initializer_list>
#include <list>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const ErrorCode LOGICAL_ERROR;
}

// this append only
template <typename Key, typename Value, typename Hash = std::hash<Key>, typename Equal = std::equal_to<Key>>
class LinkedHashMap
{
public:
    LinkedHashMap() = default;
    template <typename KeyArg, typename ValueArg>
    void emplace_back(KeyArg && key_arg, ValueArg && value_args)
    {
        emplace(std::forward<KeyArg>(key_arg), std::forward<ValueArg>(value_args));
    }

    template<typename KeyArg, typename ValueArg>
    void emplace(KeyArg&& key_arg, ValueArg&& value_args)
    {
        auto index = ordered_storage.size();
        if (mapping.count(key_arg))
        {
            throw Exception("duplicated key is not allowed", ErrorCodes::LOGICAL_ERROR);
        }
        mapping[key_arg] = index;
        ordered_storage.emplace_back(std::forward<KeyArg>(key_arg), std::forward<ValueArg>(value_args));
    }

    LinkedHashMap(std::initializer_list<std::pair<Key, Value>>&& init_list): ordered_storage(std::move(init_list)) {
        size_t index = 0;
        for(auto& [k, v]: ordered_storage)
        {
            (void)v;
            mapping[k] = index++;
        }
    }

    template <typename Iter>
    LinkedHashMap(Iter beg, Iter end)
    {
        insert_back(beg, end);
    }

    template<typename KeyArg>
    bool contains(const KeyArg & key_arg) const
    {
        return mapping.count(key_arg);
    }

    void emplace_back(const std::pair<Key, Value> & assignment)
    {
        emplace_back(assignment.first, assignment.second);
    }

    void emplace_back(std::pair<Key, Value> && assignment)
    {
        emplace_back(std::move(assignment.first), std::move(assignment.second));
    }

    template <typename Iter>
    void insert_back(Iter beg, Iter end)
    {
        for (auto iter = beg; iter != end; ++iter)
        {
            this->emplace_back(iter->first, iter->second);
        }
    }

    void erase(const Key& key) {
        auto iter = mapping.find(key);
        if (iter == mapping.end())
            return;
        auto index = iter->second;
        ordered_storage.erase(index + begin());
    }

    template<typename Iter>
    Iter erase(Iter iter)
    {
        mapping.erase(iter->first);
        return ordered_storage.erase(iter);
    }

    void clear()
    {
        ordered_storage.clear();
        mapping.clear();
    }

    // TODO: use user-defined key to avoid it
    // non-const iterate is not safe since
    // user may modify the value

    auto begin() {
        return ordered_storage.begin();
    }
    auto end() {
        return ordered_storage.end();
    }

    auto begin() const{
        return ordered_storage.cbegin();
    }
    auto end() const {
        return ordered_storage.cend();
    }

    size_t size() const {
        return ordered_storage.size();
    }

    size_t count(const Key& key) const
    {
        return mapping.count(key);
    }

    bool empty() const {
        return ordered_storage.empty();
    }

    Value& at(const Key& key) {
        auto iter = mapping.find(key);
        if (iter == mapping.end())
        {
            throw Exception("out of bounds", ErrorCodes::LOGICAL_ERROR);
        }
        auto index = iter->second;
        return ordered_storage.at(index).second;
    }

    const Value & at(const Key & key) const
    {
        return const_cast<LinkedHashMap<Key, Value, Hash, Equal> *>(this)->at(key);
    }

    const auto& front() const
    {
        return ordered_storage.front();
    }

    const auto & back() const
    {
        return ordered_storage.back();
    }

    __attribute__((__used__)) String toString() const
    {
        auto to_string = [](const auto & obj) -> String {
            using T = std::decay_t<decltype(obj)>;
            constexpr auto has_std_to_string = boost::hana::is_valid([](auto && x) -> decltype(std::to_string(x)) {});
            constexpr auto has_to_string = boost::hana::is_valid([](auto && x) -> decltype(x.toString()) {});

            if constexpr (std::is_same_v<T, String>)
            {
                return obj;
            }
            else if constexpr (std::is_same_v<T, ASTPtr> || std::is_same_v<T, ConstASTPtr>)
            {
                return serializeAST(*obj, true);
            }
            else if constexpr (decltype(has_std_to_string(obj))::value)
            {
                return std::to_string(obj);
            }
            else if constexpr (decltype(has_to_string(obj))::value)
            {
                return obj.toString();
            }
            else
            {
                return "[unserializable object]";
            }
        };

        std::stringstream os;
        for (const auto & item : ordered_storage)
            os << to_string(item.first) << " := " << to_string(item.second) << std::endl;
        return os.str();
    }

    bool operator==(const LinkedHashMap & other) const
    {
        if (mapping.size() != other.mapping.size()) {
            return false;
        }
        for (const auto & entry : mapping) {
            if (!other.count(entry.first)) {
                return false;
            }
            if (ordered_storage.at(entry.second).second != other.at(entry.first)) {
                return false;
            }
        }
        return true;
    }

    template <typename KeyArg, typename ValArg = Value, std::enable_if_t<std::is_default_constructible<ValArg>::value, bool> = true>
    Value & operator[](KeyArg && key)
    {
        auto it = mapping.emplace(key, ordered_storage.size());
        if (it.second)
            ordered_storage.emplace_back(std::forward<KeyArg>(key), ValArg());
        return ordered_storage.at(it.first->second).second;
    }

    LinkedHashMap(const LinkedHashMap &) = default;
    LinkedHashMap(LinkedHashMap &&) = default;

    LinkedHashMap& operator=(const LinkedHashMap &) = default;
    LinkedHashMap& operator=(LinkedHashMap &&) = default;
private:
    std::vector<std::pair<Key, Value>> ordered_storage;
    std::unordered_map<Key, size_t, Hash, Equal> mapping;
};

} // namespace DB
