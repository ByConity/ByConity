#pragma once

#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include "common/types.h"
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
}

inline BroadcastStatus sendAndCheckReturnStatus(IBroadcastSender & sender, Chunk chunk)
{
    BroadcastStatus status = sender.send(std::move(chunk));
    if (status.is_modifer && status.code > 0)
    {
        throw Exception(
            String(typeid(sender).name()) + " fail to send data: " + status.message + " code: " + std::to_string(status.code),
            ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
    }
    return status;
}

}
