#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <common/logger_useful.h>
#include "HaTCPHandler.h"
#include "IServer.h"

namespace Poco
{
class Logger;
}

namespace DB
{
class HaTCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;

public:
    explicit HaTCPHandlerFactory(IServer & server_, bool secure_ = false)
        : server(server_), log(&Poco::Logger::get(std::string("HaTCP") + (secure_ ? "S" : "") + "HandlerFactory"))
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        LOG_TRACE(log, "TCP Request. Address: {} ", socket.peerAddress().toString());

        return new HaTCPHandler(server, socket);
    }
};

}
