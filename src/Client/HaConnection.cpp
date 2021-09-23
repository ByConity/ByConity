#include <Client/HaConnection.h>

#include <Core/Protocol.h>
#include <Common/DNSResolver.h>
#include <Common/NetException.h>
#include <Common/ClickHouseRevision.h>
#include <Common/config_version.h>
#include <Client/TimeoutSetter.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/Net/NetException.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

HaConnection::HaConnection(String host_,
        UInt16 port_,
        String remote_replica_,
        String remote_endpoint_,
        String replica_name_,
        String user_,
        String password_,
        ConnectionTimeouts timeouts_,
        Poco::Timespan sync_request_timeout_)
    : host(std::move(host_)),
    port(port_),
    remote_replica(std::move(remote_replica_)),
    remote_endpoint(std::move(remote_endpoint_)),
    replica_name(std::move(replica_name_)),
    user(std::move(user_)),
    password(std::move(password_)),
    timeouts(std::move(timeouts_)),
    sync_request_timeout(sync_request_timeout_),
    log_wrapper(*this)
{
    if (user.empty())
        user = "default";

    setDescription();
}

void HaConnection::setDescription()
{
    auto resolved_address = getResolvedAddress();
    description = host + ":" + toString(resolved_address.port());
    auto ip_address = resolved_address.host().toString();

    if (host != ip_address)
        description += ", " + ip_address;
}

Poco::Net::SocketAddress HaConnection::getResolvedAddress() const
{
    if (connected)
        return current_resolved_address;

    return DNSResolver::instance().resolveAddress(host, port);
}

void HaConnection::connect()
{
    try
    {
        if (connected)
            disconnect();

        LOG_TRACE(
            log_wrapper.get(), "{} connecting. Replica: {}. Endpoint: {}. User: {}.", getName(), remote_replica, remote_endpoint, user);

        socket = std::make_unique<Poco::Net::StreamSocket>();

        current_resolved_address = DNSResolver::instance().resolveAddress(host, port);

        socket->connect(current_resolved_address, timeouts.connection_timeout);
        socket->setReceiveTimeout(timeouts.receive_timeout);
        socket->setSendTimeout(timeouts.send_timeout);
        socket->setNoDelay(true);
        if (timeouts.tcp_keep_alive_timeout.totalSeconds())
        {
            socket->setKeepAlive(true);
            socket->setOption(
                IPPROTO_TCP,
#if defined(TCP_KEEPALIVE)
                TCP_KEEPALIVE
#else
                TCP_KEEPIDLE // __APPLE__
#endif
                ,
                timeouts.tcp_keep_alive_timeout);
        }

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

        connected = true;

        sendHello();
        receiveHello();

        LOG_TRACE(log_wrapper.get(), "Ha Connected to {}. Server version: {}.{}.{}", server_name, server_version_major, server_version_minor, server_revision); 
    }
    catch (Poco::Net::NetException & e)
    {
        disconnect();

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(e.displayText() + "(" + getDescription() + ")", ErrorCodes::NETWORK_ERROR);
    }
    catch (Poco::TimeoutException & e)
    {
        disconnect();

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(e.displayText() + "(" + getDescription() + ")", ErrorCodes::SOCKET_TIMEOUT);
    }
}

void HaConnection::disconnect()
{
    in = nullptr;
    out = nullptr; // can write to socket
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
}

void HaConnection::sendHello()
{
    /// See dbms/src/Client/Connection.cpp, void Connection::sendHello()
    auto has_control_character = [](const std::string & s)
    {
        for (auto c : s)
            if (isControlASCII(c))
                return true;
        return false;
    };

    if (has_control_character(remote_replica)
        || has_control_character(remote_endpoint)
        || has_control_character(user)
        || has_control_character(password))
        throw Exception("Parameters 'replica', 'endpoint', 'user' and 'password' must not contain ASCII control characters",
                        ErrorCodes::BAD_ARGUMENTS);

    writeVarUInt(Protocol::HaClient::Hello, *out);
    writeStringBinary(DBMS_NAME, *out);
    writeVarUInt(DBMS_VERSION_MAJOR, *out);
    writeVarUInt(DBMS_VERSION_MINOR, *out);
    writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, *out);

    writeStringBinary(remote_replica, *out);
    writeStringBinary(remote_endpoint, *out);
    writeStringBinary(replica_name, *out);
    writeStringBinary(user, *out);
    writeStringBinary(password, *out);

    out->next();
}

void HaConnection::receiveHello()
{
    receivePacketTypeOrThrow(Protocol::HaServer::Hello);

    readStringBinary(server_name, *in);
    readVarUInt(server_version_major, *in);
    readVarUInt(server_version_minor, *in);
    readVarUInt(server_revision, *in);
}

void HaConnection::ping()
{
    /// send ping
    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);
    writeVarUInt(Protocol::HaClient::Ping, *out);
    out->next();

    receivePacketTypeOrThrow(Protocol::HaServer::Pong);
}

void HaConnection::receiveOK()
{
    receivePacketTypeOrThrow(Protocol::HaServer::OK);
}

std::unique_ptr<Exception> HaConnection::receiveException()
{
    return std::make_unique<Exception>(readException(*in, "Received from " + getDescription(), true /* remote */));
}

void HaConnection::throwUnexpectedPacket(UInt64 packet_type, const String & expected) const
{
    throw NetException("Unexpected packet from server " + getDescription() + " (expected " + expected
            + ", got " + String(Protocol::HaServer::toString(packet_type)) + " " + toString(packet_type) + ")",
            ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}

void HaConnection::receivePacketTypeOrThrow(UInt64 expected)
{
    UInt64 packet_type {0};
    readVarUInt(packet_type, *in);
    if (packet_type == expected)
    {
        /// do nothing
    }
    else if (packet_type == Protocol::HaServer::Exception)
    {
        receiveException()->rethrow();
    }
    else
    {
        throwUnexpectedPacket(packet_type, String(Protocol::HaServer::toString(expected)));
    }

}

} // end of namespace DB
