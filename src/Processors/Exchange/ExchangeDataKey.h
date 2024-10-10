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
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <common/types.h>

namespace DB
{
struct ExchangeDataKey
{
    explicit ExchangeDataKey(
        UInt64 query_unique_id_, UInt64 exchange_id_, UInt64 partition_id_, UInt64 parallel_index_ = std::numeric_limits<UInt64>::max())
        : query_unique_id(query_unique_id_), exchange_id(exchange_id_), partition_id(partition_id_), parallel_index(parallel_index_)
    {
    }
    UInt64 query_unique_id;
    UInt64 exchange_id;
    // id of output partition
    UInt64 partition_id;
    // index of this task
    UInt64 parallel_index;

    String toString() const
    {
        return fmt::format("{}", *this);
    }

    bool operator==(const ExchangeDataKey & other) const
    {
        return query_unique_id == other.query_unique_id && exchange_id == other.exchange_id && partition_id == other.partition_id
            && parallel_index == other.parallel_index;
    }

    bool operator<(const ExchangeDataKey & other) const
    {
        if (query_unique_id != other.query_unique_id)
            return query_unique_id < other.query_unique_id;
        if (exchange_id != other.exchange_id)
            return exchange_id < other.exchange_id;
        if (partition_id != other.partition_id)
            return partition_id < other.partition_id;
        if (parallel_index != other.parallel_index)
            return parallel_index < other.parallel_index;
        return false;
    }
};

using ExchangeDataKeyPtr = std::shared_ptr<ExchangeDataKey>;
using ExchangeDataKeyPtrs = std::vector<ExchangeDataKeyPtr>;

/// exchange data key with extra information
struct ExtendedExchangeDataKey
{
    ExchangeDataKeyPtr key;
    UInt64 write_segment_id;
    UInt64 read_segment_id;
    ExchangeMode exchange_mode;
};

struct ExchangeDataKeyHashFunc
{
    size_t operator()(const ExchangeDataKey & key) const
    {
        size_t h1 = std::hash<UInt64>()(key.query_unique_id);
        size_t h2 = std::hash<UInt64>()(key.exchange_id);
        size_t h3 = std::hash<UInt64>()(key.partition_id);
        size_t h4 = std::hash<UInt64>()(key.parallel_index);
        return h1 ^ h2 ^ h3 ^ h4;
    }
};

struct ExchangeDataKeyPtrComp
{
public:
    size_t operator()(const ExchangeDataKeyPtr & key) const
    {
        return ExchangeDataKeyHashFunc()(*key);
    }
    bool operator()(const ExchangeDataKeyPtr & lhs, const ExchangeDataKeyPtr & rhs) const
    {
        return *lhs == *rhs;
    }
};

struct ExchangeDataKeyPtrLess
{
    bool operator()(const ExchangeDataKeyPtr & lhs, const ExchangeDataKeyPtr & rhs) const
    {
        return *lhs < *rhs;
    }
};
}
template <>
struct fmt::formatter<DB::ExchangeDataKey>
{
    constexpr auto parse(format_parse_context & ctx)
    {
        const auto *it = ctx.begin();
        const auto *end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("Invalid format for struct ExchangeDataKey");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::ExchangeDataKey & key, FormatContext & ctx)
    {
        return format_to(
            ctx.out(), "ExchangeDataKey[{}_{}_{}_{}]", key.query_unique_id, key.exchange_id, key.partition_id, key.parallel_index);
    }
};
