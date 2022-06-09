#pragma once

#include <TSO/TSOProxy.h>
#include <Protos/tso.pb.h>
#include <Poco/Logger.h>
#include <atomic>

namespace DB
{

namespace TSO
{


constexpr size_t TSO_BITS = 64;
constexpr size_t LOGICAL_BITS = 18;
constexpr size_t MAX_LOGICAL = 1 << LOGICAL_BITS;
constexpr size_t LOGICAL_BITMASK = 0x3FFFF;
constexpr size_t TSO_UPDATE_INTERVAL = 50;  /// 50 milliseconds

#define ts_to_physical(ts) ((ts) >> LOGICAL_BITS)
#define ts_to_logical(ts) ((ts) & LOGICAL_BITMASK)
#define physical_logical_to_ts(physical, logical) ((physical << LOGICAL_BITS) | (logical & LOGICAL_BITMASK))

struct TSOClock
{
    UInt64 physical;
    UInt32 logical;
};

class TSOImpl : public DB::TSO::TSO {

public:
    TSOImpl() {}

    ~TSOImpl() {}

    void setPhysicalTime(UInt64 time);

    TSOClock getClock() const
    {
        UInt64 timestamp = ts.load(std::memory_order_acquire);
        TSOClock clock = {ts_to_physical(timestamp), UInt32 ts_to_logical(timestamp)};
        return clock;
    }

    void GetTimestamp(::google::protobuf::RpcController* /*controller*/,
                        const ::DB::TSO::GetTimestampReq* request,
                        ::DB::TSO::GetTimestampResp* response,
                        ::google::protobuf::Closure* done);

    void GetTimestamps(::google::protobuf::RpcController* /*controller*/,
                        const ::DB::TSO::GetTimestampsReq* request,
                        ::DB::TSO::GetTimestampsResp* response,
                        ::google::protobuf::Closure* done);

private:
    std::atomic<UInt64> ts = 0;
    Poco::Logger * log = &Poco::Logger::get("TSOImpl");
    std::atomic<bool> logical_clock_checking {false};

    void checkLogicalClock(UInt32 logical_value);
    UInt64 fetchAddLogical(UInt32 to_add);
};

}

}
