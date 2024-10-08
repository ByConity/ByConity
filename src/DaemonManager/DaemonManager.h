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

#pragma once

#include <Common/Logger.h>
#include <Server/IServer.h>
#include <Poco/Net/ServerSocket.h>
#include <daemon/BaseDaemon.h>
#include <Server/HTTP/HTTPServer.h>

namespace DB::DaemonManager
{

class DaemonManager : public BaseDaemon, public IServer
{
public:
    using ServerApplication::run;

    Poco::Util::LayeredConfiguration & config() const override
    {
        return BaseDaemon::config();
    }

    Poco::Logger & logger() const override
    {
        return BaseDaemon::logger();
    }

    ContextMutablePtr context() const override
    {
        return global_context;
    }

    bool isCancelled() const override
    {
        return BaseDaemon::isCancelled();
    }

    void defineOptions(Poco::Util::OptionSet & _options) override;

protected:
    int run() override;

    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

    std::string getDefaultCorePath() const override;

private:
    std::vector<std::unique_ptr<HTTPServer>> http_servers;

    /**
     * @brief Binds metrics HTTP server to the given hosts and port.
     * After binding, caller need to explicitly start the servers.
     *
     * @param host Addresses to bind to.
     * @param port Port number.
     * @param server_pool Poco Thread pool for handling requests.
     * @param listen_try If true, catch and log errors. Throws otherwise.
     */
    void initMetricsHandler(const std::vector<String> & host, const UInt16 & port, Poco::ThreadPool & server_pool, bool listen_try);
    /// A copied logic to bind and listen a socket.
    Poco::Net::SocketAddress socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure = false) const;
    ContextMutablePtr global_context;
};

}
