#pragma once

#include <memory>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <absl/strings/str_split.h>
#include <Common/Allocator.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
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
            .exhcange_timeout_ms = static_cast<UInt32>(settings.exchange_timeout_ms),
            .send_threshold_in_bytes = settings.exchange_buffer_send_threshold_in_bytes,
            .send_threshold_in_row_num = settings.exchange_buffer_send_threshold_in_row,
            .force_remote_mode = settings.exchange_enable_force_remote_mode};
    }

    static inline DataTransKeyPtr parseDataKey(const String & key_str) noexcept
    {
        std::vector<std::string> elements = absl::StrSplit(key_str, '_');
        if (elements.size() != 5)
            return DataTransKeyPtr();
        return std::make_shared<ExchangeDataKey>(elements[0], elements[1], elements[2], elements[3], elements[4]);
    }

    static inline BroadcastStatus sendAndCheckReturnStatus(IBroadcastSender & sender, Chunk chunk)
    {
        BroadcastStatus status = sender.send(std::move(chunk));
        if (status.is_modifer && status.code > 0)
        {
            throw Exception(
                sender.getName() + " fail to send data: " + status.message + " code: " + std::to_string(status.code),
                ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
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
};

}
