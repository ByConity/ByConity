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

#include <string>
#include <fmt/core.h>

namespace DB
{
struct ExchangeDataKey
{
    const String query_id;
    const UInt64 exchange_id;
    const UInt64 parallel_index;
    const String coordinator_address;

    ExchangeDataKey(
        String query_id_, UInt64 exchange_id_, UInt64 parallel_index_, String coordinator_address_)
        : query_id(std::move(query_id_))
        , exchange_id(exchange_id_)
        , parallel_index(parallel_index_)
        , coordinator_address(std::move(coordinator_address_))
    {
    }

    bool operator==(const ExchangeDataKey & compare_to) const
    {
        return (
            exchange_id == compare_to.exchange_id &&
            parallel_index == compare_to.parallel_index &&
            0 == query_id.compare(compare_to.query_id) &&
            0 == coordinator_address.compare(coordinator_address)
        );
    }

    bool operator<(const ExchangeDataKey & compare_to) const
    {
        if (exchange_id < compare_to.exchange_id)
            return true;
        if (exchange_id > compare_to.exchange_id)
            return false;

        if (parallel_index < compare_to.parallel_index)
            return true;
        if (parallel_index > compare_to.parallel_index)
            return false;

        int query_id_compare = query_id.compare(compare_to.query_id);
        if (query_id_compare < 0)
            return true;
        if (query_id_compare > 0)
            return false;

        int coordinator_address_compare = coordinator_address.compare(compare_to.coordinator_address);
        if (coordinator_address_compare < 0)
            return true;
        if (coordinator_address_compare > 0)
            return false;

        return true;
    }

    String getKey() const
    {
        return query_id + "_" + std::to_string(exchange_id) + "_"
            + std::to_string(parallel_index) + "_" + coordinator_address;
    }

    String dump() const
    {
        return fmt::format(
            "ExchangeDataKey: [query_id: {}, exchange_id: {}, parallel_index: {}, coordinator_address: {}]",
            query_id,
            exchange_id,
            parallel_index,
            coordinator_address);
    }

    inline const String & getQueryId() const { return query_id; }

    inline const String & getCoordinatorAddress() const { return coordinator_address; }

    inline UInt64 getExchangeId() const {return exchange_id;}

    inline UInt64 getParallelIndex() const {return parallel_index;}
};

using ExchangeDataKeyPtr = std::shared_ptr<ExchangeDataKey>;
using ExchangeDataKeyPtrs = std::vector<ExchangeDataKeyPtr>;

struct ExchangeDataKeyHashFunc
{
    size_t operator()(const ExchangeDataKey & key) const
    {
        size_t h1 = std::hash<int>()(key.exchange_id);
        size_t h2 = std::hash<int>()(key.parallel_index);
        size_t h3 = std::hash<String>()(key.query_id);
        size_t h4 = std::hash<String>()(key.coordinator_address);

        return h1 ^ h2 ^ h3 ^ h4;
    }
};

}
