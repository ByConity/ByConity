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

#include <memory>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <absl/strings/str_split.h>
#include <fmt/core.h>
#include <Common/CurrentThread.h>
#include <Common/Allocator.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/time.h>
#include <common/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
    extern const int BRPC_EXCEPTION;
    extern const int TIMEOUT_EXCEEDED;
}
class ExchangeUtils
{
public:
    static inline bool isLocalExchange(const AddressInfo & read_address_info, const AddressInfo & write_address_info)
    {
        return static_cast<bool>(
            (read_address_info.getHostName() == "localhost" && read_address_info.getPort() == 0)
            || (write_address_info.getHostName() == "localhost" && write_address_info.getPort() == 0)
            || read_address_info == write_address_info);
    }

    static inline ExchangeOptions getExchangeOptions(const ContextPtr & context)
    {
        const auto & settings = context->getSettingsRef();
        return {
            .exchange_timeout_ts = context->getQueryExpirationTimeStamp(),
            .send_threshold_in_bytes = settings.exchange_buffer_send_threshold_in_bytes,
            .send_threshold_in_row_num = settings.exchange_buffer_send_threshold_in_row,
            .force_remote_mode = settings.exchange_enable_force_remote_mode,
            .force_use_buffer = settings.exchange_force_use_buffer};
    }

    static inline BroadcastStatus sendAndCheckReturnStatus(IBroadcastSender & sender, Chunk chunk)
    {
        BroadcastStatus status = sender.send(std::move(chunk));
        if (status.is_modified_by_operator && status.code > 0)
        {
            if(status.code == BroadcastStatusCode::SEND_TIMEOUT)
            {
                throw Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED,
                    "Query {} send data timeout, maybe you can increase settings max_execution_time. Debug info: sender {}, msg : {}",
                    CurrentThread::getQueryId(),
                    sender.getName(),
                    status.message);

            }
            else 
            {
                throw Exception(
                    ErrorCodes::BRPC_EXCEPTION,
                    "Query {} cancel sending data: sender {}, code: {}, msg : {}",
                    CurrentThread::getQueryId(),
                    sender.getName(),
                    status.code,
                    status.message);
            }
        }
        return status;
    }

    static inline void mergeSenders(BroadcastSenderPtrs & senders)
    {
        BroadcastSenderPtrs senders_to_merge;
        for (auto it = senders.begin(); it != senders.end();)
        {
            if ((*it)->getType() == BroadcastSenderType::Brpc)
            {
                senders_to_merge.emplace_back(std::move(*it));
                it = senders.erase(it);
            }
            else
                it++;
        }
        if (senders_to_merge.empty())
            return;
        auto & merged_sender = senders_to_merge[0];
        for (size_t i = 1; i < senders_to_merge.size(); ++i)
        {
            merged_sender->merge(std::move(*senders_to_merge[i]));
        }
        senders.emplace_back(std::move(merged_sender));
    }

    static inline void transferGlobalMemoryToThread(Int64 bytes)
    {
        if (DB::MainThreadStatus::get())
            total_memory_tracker.free(bytes);
        CurrentMemoryTracker::alloc(bytes);
    }
};

}
