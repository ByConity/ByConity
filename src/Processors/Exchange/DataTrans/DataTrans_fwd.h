#pragma once

#include <memory>
#include <vector>
#include "common/types.h"

namespace DB
{
class IBroadcastReceiver;
class IBroadcastSender;
using BroadcastReceiverPtr = std::shared_ptr<IBroadcastReceiver>;
using BroadcastSenderPtr = std::shared_ptr<IBroadcastSender>;
using BroadcastSenderPtrs = std::vector<BroadcastSenderPtr>;

/// Status code indicates the status of the broadcaster which consists by connected senders and receiver.
/// We should cancel data transport immediately when return positive status code and close gracefully when meet negative code.
/// Close gracefully means that no data can be send any more but in flight data shoule be consumed.
enum BroadcastStatusCode
{
    ALL_SENDERS_DONE = -1,
    RUNNING = 0,
    RECV_REACH_LIMIT = 1,
    RECV_TIMEOUT = 2,
    SEND_TIMEOUT = 3,
    RECV_CANCELLED = 4,
    SEND_CANCELLED = 5,
    RECV_UNKNOWN_ERROR = 99,
    SEND_UNKNOWN_ERROR = 100
};

struct BroadcastStatus
{
    explicit BroadcastStatus(BroadcastStatusCode status_code_) : code(status_code_), is_modifer(false){ }

    explicit BroadcastStatus(BroadcastStatusCode status_code_, bool is_modifer_) : code(status_code_), is_modifer(is_modifer_) { }

    explicit BroadcastStatus(BroadcastStatusCode status_code_, bool is_modifer_, String message_)
        : code(status_code_), is_modifer(is_modifer_), message(std::move(message_))
    {
    }

    BroadcastStatusCode code;

    /// Is this operation modified the status
    mutable bool is_modifer;

    /// message about why changed to this status
    String message;

    /// The one who change the status lastly
    String modifer;

    UInt64 time;


};


}
