/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <memory>
#include <Poco/Net/TCPServerConnection.h>

#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <IO/Progress.h>
#include <IO/TimeoutSetter.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <common/getFQDNOrHostName.h>

#include "IO/ReadBuffer.h"
#include "IServer.h"
#include "Interpreters/Context_fwd.h"
#include "TCPQuery.h"


namespace CurrentMetrics
{
extern const Metric TCPConnection;
}

namespace Poco
{
class Logger;
}

namespace DB
{

class ColumnsDescription;


class TCPHandler : public Poco::Net::TCPServerConnection
{
public:
    /** parse_proxy_protocol_ - if true, expect and parse the header of PROXY protocol in every connection
      * and set the information about forwarded address accordingly.
      * See https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
      *
      * Note: immediate IP address is always used for access control (accept-list of IP networks),
      *  because it allows to check the IP ranges of the trusted proxy.
      * Proxy-forwarded (original client) IP address is used for quota accounting if quota is keyed by forwarded IP.
      */
    TCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_, bool parse_proxy_protocol_, std::string server_display_name_);
    ~TCPHandler() override;

    void run() override;

private:
    IServer & server;
    bool parse_proxy_protocol = false;
    Poco::Logger * log;

    ClientVersionInfo client_info;
    InterServerInfo server_info;

    ContextMutablePtr connection_context;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    String default_database;

    /// Store current query for sending errors or logs to client.
    std::shared_ptr<TCPQuery> current_query;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};

    /// It is the name of the server that will be sent to the client.
    String server_display_name;

    void runImpl();

    bool receiveProxyHeader();
    void receiveHello();

    void receiveClusterNameAndSalt();

    void sendHello();

    void sendException(const Exception & e, bool with_stack_trace);
    void sendEndOfStream();

    bool isAsyncMode();
};
}
