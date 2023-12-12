#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
String toString(BroadcastStatusCode code)
{
    String code_string = "UNKNOWN code:";
    switch (code)
    {
        case DB::BroadcastStatusCode::ALL_SENDERS_DONE:
            code_string = "ALL_SENDERS_DONE";
            break;
        case DB::BroadcastStatusCode::RUNNING:
            code_string = "RUNNING";
            break;
        case DB::BroadcastStatusCode::RECV_REACH_LIMIT:
            code_string = "RECV_REACH_LIMIT";
            break;
        case DB::BroadcastStatusCode::RECV_TIMEOUT:
            code_string = "RECV_TIMEOUT";
            break;
        case DB::BroadcastStatusCode::SEND_TIMEOUT:
            code_string = "SEND_TIMEOUT";
            break;
        case DB::BroadcastStatusCode::RECV_CANCELLED:
            code_string = "RECV_CANCELLED";
            break;
        case DB::BroadcastStatusCode::SEND_CANCELLED:
            code_string = "SEND_CANCELLED";
            break;
        case DB::BroadcastStatusCode::RECV_NOT_READY:
            code_string = "RECV_NOT_READY";
            break;
        case DB::BroadcastStatusCode::SEND_NOT_READY:
            code_string = "SEND_NOT_READY";
            break;
        case DB::BroadcastStatusCode::RECV_UNKNOWN_ERROR:
            code_string = "RECV_UNKNOWN_ERROR";
            break;
        case DB::BroadcastStatusCode::SEND_UNKNOWN_ERROR:
            code_string = "SEND_UNKNOWN_ERROR";
            break;
        default:
            code_string = fmt::format("Unknown code:{}", static_cast<int>(code));
    }
    return code_string;
}
}
