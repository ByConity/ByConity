#include "HaTCPHandler.h"

#include <Core/Protocol.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/HaReplicaHandler.h>
#include <Common/ClickHouseRevision.h>
#include <Common/NetException.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>

#include <Poco/Net/NetException.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int DUPLICATE_HA_CLIENT;
    extern const int BAD_ARGUMENTS;
}

void HaTCPHandler::run()
{
    try
    {
        runImpl();

        LOG_DEBUG(log, "Done processing Ha Connection from {}, replica {}, endpoint {}.", socket().peerAddress().toString(), replica, endpoint_name);
    }
    catch (Poco::Exception & e)
    {
        /// Timeout - not an error.
        if (!strcmp(e.what(), "Timeout"))
        {
            LOG_DEBUG(log, "Poco::Exception. Code: {}, e.code() = {}, e.displayText() = {}, e.what() = {}", ErrorCodes::POCO_EXCEPTION, e.code(), e.displayText(), e.what());
        }
        else
            throw;
    }
}

void HaTCPHandler::runImpl()
{
    setThreadName("HaTCPHandler");
    ThreadStatus thread_status;

    connection_context = Context::createCopy(server.context());
    connection_context->makeSessionContext();

    Settings global_settings = connection_context->getSettings();

    socket().setReceiveTimeout(global_settings.receive_timeout);
    socket().setSendTimeout(global_settings.send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return;
    }

    HaReplicaEndpointPtr endpoint;
    try
    {
        receiveHello();

        endpoint = connection_context->getHaReplicaHandler().getEndpoint(endpoint_name);
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        if (e.code() == ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT)
        {
            LOG_DEBUG(log, "Client has connected to wrong port.");
            return;
        }

        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
        {
            LOG_WARNING(log, "Client has gone away.");
            return;
        }

        /// We try to send error information to the client.
        sendExceptionNoThrow(e);
        throw;
    }

    sendHello();

    while (1)
    {
        /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
        while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(global_settings.poll_interval * 1000000) && !server.isCancelled()
               && !endpoint->isCancelled())
        {
        }

        if (server.isCancelled() || endpoint->isCancelled() || in->eof())
            break;

        std::unique_ptr<Exception> exception;
        bool network_error{false};

        try
        {
            UInt64 packat_type = 0;
            readVarUInt(packat_type, *in);
            endpoint->processPacket(packat_type, *in, *out);
        }
        catch (const Exception & e)
        {
            exception.reset(e.clone());

            if (e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT)
                throw;

            /// If a timeout occurred, try to inform client about it and close the session
            if (e.code() == ErrorCodes::SOCKET_TIMEOUT)
                network_error = true;
        }
        catch (const Poco::Net::NetException & e)
        {
            /** We can get here if there was an error during connection to the client,
             *  or in connection with a remote server that was used to process the request.
             *  It is not possible to distinguish between these two cases.
             *  Although in one of them, we have to send exception to the client, but in the other - we can not.
             *  We will try to send exception to the client in any case - see below.
             */
            exception = std::make_unique<Exception>(e.displayText(), ErrorCodes::POCO_EXCEPTION);
        }
        catch (const Poco::Exception & e)
        {
            exception = std::make_unique<Exception>(e.displayText(), ErrorCodes::POCO_EXCEPTION);
        }
        catch (const std::exception & e)
        {
            exception = std::make_unique<Exception>(e.what(), ErrorCodes::STD_EXCEPTION);
        }
        catch (...)
        {
            exception = std::make_unique<Exception>("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
        }

        try
        {
            if (exception)
                sendException(*exception);
        }
        catch (...)
        {
            /** Could not send exception information to the client. */
            network_error = true;
            LOG_WARNING(log, "Client has gone away.");
        }

        if (network_error)
            break;
    }
}

void HaTCPHandler::receiveHello()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);
    if (packet_type != Protocol::HaClient::Hello)
    {
        /** If you accidentally accessed the HTTP protocol for a port destined for an internal TCP protocol,
          * Then instead of the packet type, there will be G (GET) or P (POST), in most cases.
          */
        if (packet_type == 'G' || packet_type == 'P')
        {
            writeString(
                "HTTP/1.0 400 Bad Request\r\n\r\n"
                "Port "
                    + server.config().getString("ha_tcp_port") + " is for ha log exchanging.\r\n" + "You must use port "
                    + server.config().getString("http_port") + " for HTTP.\r\n",
                *out);

            throw Exception("Client has connected to wrong port", ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT);
        }
        else
            throw NetException("Unexpected packet from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    }

    readStringBinary(client_name, *in);
    readVarUInt(client_version_major, *in);
    readVarUInt(client_version_minor, *in);
    readVarUInt(client_tcp_protocol_version, *in);

    readStringBinary(replica, *in);
    readStringBinary(endpoint_name, *in);
    readStringBinary(client_replica, *in);

    String user = "default";
    String password;
    readStringBinary(user, *in);
    readStringBinary(password, *in);

    LOG_DEBUG(
        log,
        "HaTCP connected {} version {}.{}.{}, revision {}, replica {}, endpoint {}, client_replica {}, user {}",
        client_name,
        client_version_major,
        client_version_minor,
        client_version_patch,
        client_tcp_protocol_version,
        replica,
        endpoint_name,
        client_replica,
        user);

}

void HaTCPHandler::sendHello()
{
    writeVarUInt(Protocol::HaServer::Hello, *out);
    writeStringBinary(DBMS_NAME, *out);
    writeVarUInt(DBMS_VERSION_MAJOR, *out);
    writeVarUInt(DBMS_VERSION_MINOR, *out);
    writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, *out);
    out->next();
}

void HaTCPHandler::sendException(const Exception & e)
{
    writeVarUInt(Protocol::HaServer::Exception, *out);
    writeException(e, *out, false);
    out->next();
}

void HaTCPHandler::sendExceptionNoThrow(const Exception & e)
{
    try
    {
        sendException(e);
    }
    catch (...)
    {
    }
}

} // end of namespace DB
