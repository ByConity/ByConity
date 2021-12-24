#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Processors/Exchange/ExchangeOptions.h>

namespace DB
{
class ExchangeUtils
{
public:
    static inline bool isLocalExchange(const AddressInfo & read_address_info, const AddressInfo & write_address_info)
    {
        return static_cast<bool>(
            (read_address_info.getHostName() == "localhost" && read_address_info.getPort() == 0)
            || read_address_info.toString() == write_address_info.toString());
    }

    static inline ExchangeOptions getExchangeOptions(const ContextPtr & context)
    {
        const auto & settings = context->getSettingsRef();
        return {
            .exhcange_timeout_ms = static_cast<UInt32>(settings.exchange_timeout_ms),
            .send_threshold_in_bytes = settings.exchange_buffer_send_threshold_in_bytes,
            .send_threshold_in_row_num = settings.exchange_buffer_send_threshold_in_row,
            .local_debug_mode = settings.exchange_enable_local_debug_mode};
    }
};

}
