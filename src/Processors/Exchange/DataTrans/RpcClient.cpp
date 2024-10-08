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

#include "RpcClient.h"

#include <errno.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <fmt/core.h>
#include <Common/Exception.h>

namespace DB
{

RpcClient::RpcClient(String host_port_, std::function<void()> report_err_, brpc::ChannelOptions * options)
    : log(getLogger("RpcClient"))
    , host_port(std::move(host_port_))
    , report_err(std::move(report_err_))
    , brpc_channel(std::make_unique<brpc::Channel>())
{
    initChannel(*brpc_channel, host_port, options);
}

void RpcClient::checkAliveWithController(const brpc::Controller & cntl) noexcept
{
    if (cntl.Failed())
    {
        auto err = cntl.ErrorCode();
        if (err == ECONNREFUSED || err == ECONNRESET || err == ENOTCONN)
            setOk(false);
        else if (err == EHOSTDOWN || err == ENETUNREACH)
            reportError();
    }
    else
    {
        setOk(true);
    }
}

void RpcClient::assertController(const brpc::Controller & cntl, int error_code)
{
    if (cntl.Failed())
    {
        auto err = cntl.ErrorCode();
        if (err == ECONNREFUSED || err == ECONNRESET)
            setOk(false);
        else if (err == EHOSTDOWN || err == ENETUNREACH || err == ENOTCONN)
            reportError();
        throw Exception(
            fmt::format("Fail to call {}, error code: {}, msg: {}", cntl.method()->full_name(), err, cntl.ErrorText()), error_code);
    }
    else
    {
        setOk(true);
    }
}

void RpcClient::initChannel(brpc::Channel & channel_, const String host_port_, brpc::ChannelOptions * options)
{
    if (0 != channel_.Init(host_port_.c_str(), options))
        throw Exception("Failed to initialize RPC channel to " + host_port_, ErrorCodes::BRPC_CANNOT_INIT_CHANNEL);

    LOG_TRACE(log, "Create rpc channel listening on : {}", host_port_);
}

}
