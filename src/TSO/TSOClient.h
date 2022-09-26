#pragma once
#include <CloudServices/RpcClientBase.h>
#include <Protos/tso.pb.h>

namespace DB::TSO
{
class TSO_Stub;

class TSOClient : public RpcClientBase
{
public:
    static String getName() { return "TSOClient"; }

    TSOClient(String host_port_);
    TSOClient(HostWithPorts host_ports_);
    ~TSOClient() override;

    GetTimestampResp getTimestamp();
    GetTimestampsResp getTimestamps(UInt32 size);

private:
    void assertRPCSuccess(brpc::Controller & cntl, int status);

    std::unique_ptr<TSO_Stub> stub;
};

using TSOClientPtr = std::shared_ptr<TSOClient>;

enum class TSORequestType
{
    GetTimestamp,
    GetTimestamps
};

inline std::string typeToString(TSORequestType type)
{
    switch (type)
    {
        case TSORequestType::GetTimestamp:
            return "GetTimestamp";
        case TSORequestType::GetTimestamps:
            return "GetTimestamps";
    }

    __builtin_unreachable();
}

UInt64 getTSOResponse(const Context & context, TSORequestType type, size_t size = 1);

}
