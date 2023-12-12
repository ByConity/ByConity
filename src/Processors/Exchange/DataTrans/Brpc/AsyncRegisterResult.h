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

#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Protos/registry.pb.h>
#include <brpc/stream.h>
#include <Common/Brpc/BrpcAsyncResultHolder.h>

#include <memory>

namespace DB
{
using AsyncRegisterResult = BrpcAsyncResultHolder<Protos::RegistryRequest, Protos::RegistryResponse>;
}

template <>
struct fmt::formatter<DB::Protos::RegistryRequest>
{
    constexpr auto parse(format_parse_context & ctx)
    {
        const auto it = ctx.begin();
        const auto end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("Invalid format for struct Protos::RegistryRequest");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::Protos::RegistryRequest & request, FormatContext & ctx)
    {
        return format_to(
            ctx.out(), "[{}_{}_{}-{}]", request.query_unique_id(), request.exchange_id(), request.parallel_id(), request.query_id());
    }
};
