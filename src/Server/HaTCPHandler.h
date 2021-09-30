#pragma once
#include <Poco/Net/TCPServerConnection.h>

#include <Interpreters/Context.h>
#include <Server/IServer.h>
#include <common/logger_useful.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class HaTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    HaTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
        : Poco::Net::TCPServerConnection(socket_)
        , server(server_)
        , log(&Poco::Logger::get("HaTCPHandler"))
        , connection_context(server.context())
    {
        //
    }

    void run() final;

private:
    IServer & server;
    Poco::Logger * log;

    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    UInt64 client_tcp_protocol_version = 0;

    ContextMutablePtr connection_context;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    String replica;
    String endpoint_name;
    String client_replica;

private:
    void runImpl();

    void receiveHello();
    void sendHello();

    void sendException(const Exception & e);
    void sendExceptionNoThrow(const Exception & e);
};

} // end of namespace DB
