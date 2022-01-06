#pragma once

#include <memory>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Common/Allocator.h>
#include <Common/Exception.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <absl/strings/str_split.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
};

}
