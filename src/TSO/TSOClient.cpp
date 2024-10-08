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
#include <Common/Configurations.h>
#include <TSO/TSOClient.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Protos/tso.pb.h>
#include <Protos/RPCHelpers.h>
#include <TSO/TSOImpl.h>
#include <Interpreters/Context.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <thread>

namespace ProfileEvents
{
    extern const Event TSORequest;
    extern const Event TSORequestMicroseconds;
    extern const Event TSOError;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BRPC_TIMEOUT;
    extern const int TSO_INTERNAL_ERROR;
    extern const int TSO_OPERATION_ERROR;
}

namespace TSO
{
TSOClient::TSOClient(String host_port_)
    : RpcClientBase(getName(), std::move(host_port_)), stub(std::make_unique<TSO_Stub>(&getChannel()))
{
}

TSOClient::TSOClient(HostWithPorts host_ports_)
    : RpcClientBase(getName(), std::move(host_ports_)), stub(std::make_unique<TSO_Stub>(&getChannel()))
{
}

TSOClient::~TSOClient() = default;

GetTimestampResp TSOClient::getTimestamp()
{
    GetTimestampReq req;
    GetTimestampResp resp;
    brpc::Controller cntl;

    stub->GetTimestamp(&cntl, &req, &resp, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(resp);

    return resp;
}

GetTimestampsResp TSOClient::getTimestamps(UInt32 size)
{
    GetTimestampsReq req;
    GetTimestampsResp resp;
    brpc::Controller cntl;

    req.set_size(size);
    stub->GetTimestamps(&cntl, &req, &resp, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(resp);

    return resp;
}

UInt64 getTSOResponse(const Context & context, TSORequestType type, size_t size)
{
    static auto log = getLogger("getTSOResponse");
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TSORequestMicroseconds);

    const auto & config = context.getRootConfig();
    int tso_max_retry = config.tso_service.tso_max_retry_count;

    int retry = tso_max_retry;
    bool try_update_leader = true;

    while (retry--)
    {
        ProfileEvents::increment(ProfileEvents::TSORequest);
        try
        {
            auto tso_client = context.getCnchTSOClient();

            switch (type)
            {
                case TSORequestType::GetTimestamp:
                {
                    auto response = tso_client->getTimestamp();
                    if (response.is_leader())
                        return response.timestamp();
                    break;
                }
                case TSORequestType::GetTimestamps:
                {
                    auto response = tso_client->getTimestamps(size);
                    if (response.is_leader())
                        return response.max_timestamp();
                    break;
                }
            }

            context.updateTSOLeaderHostPort();
        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::TSOError);
            if (getCurrentExceptionCode() != ErrorCodes::BRPC_TIMEOUT)
            {
                /// old leader may be unavailable
                context.updateTSOLeaderHostPort();
                try_update_leader = false;
            }

            LOG_ERROR(
                log,
                "TSO request: {} failed. Retries: {}/{}, Error message: {}",
                typeToString(type), tso_max_retry - retry, tso_max_retry, getCurrentExceptionMessage(false));
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }

    if (try_update_leader)
        context.updateTSOLeaderHostPort();

    throw Exception(ErrorCodes::TSO_OPERATION_ERROR, "Can't get process TSO request, type: {}", typeToString(type));
}

}

}
