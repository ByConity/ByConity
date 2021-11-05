#pragma once

#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#include <Poco/Net/StreamSocket.h>

#include <memory>

namespace DB
{
class HaConnection : private boost::noncopyable
{
public:
    HaConnection(
        String host_,
        UInt16 port_,
        String remote_replica_,
        String remote_endpoint_,
        String replica_name_,
        String user_,
        String password_,
        ConnectionTimeouts timeouts_,
        Poco::Timespan sync_request_timeout_ = Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0));

    virtual ~HaConnection() { disconnect(); }

    virtual String getName() { return "HaConnection"; }

    void connect();
    void disconnect();
    void sendHello();
    void receiveHello();
    void ping();
    void receiveOK();

    std::unique_ptr<Exception> receiveException();
    [[noreturn]] void throwUnexpectedPacket(UInt64 packet_type, const String & expected) const;
    void receivePacketTypeOrThrow(UInt64 expected);

    /// Returns initially resolved address
    Poco::Net::SocketAddress getResolvedAddress() const;

    const String & getDescription() const { return description; }
    const String & getHost() const { return host; }
    UInt16 getPort() const { return port; }
    const String & getRemoteReplica() const { return remote_replica; }
    const String & getRemoteEndpoint() const { return remote_endpoint; }
    const String & getUser() const { return user; }

    /// Logger is created lazily, for avoid to run DNS request in constructor.
    class LoggerWrapper
    {
    public:
        explicit LoggerWrapper(HaConnection & parent_) : parent(parent_), log(nullptr) { }
        Poco::Logger * get()
        {
            if (!log)
                log = &Poco::Logger::get("HaConnection (" + parent.getDescription() + ")");
            return log;
        }

    private:
        HaConnection & parent;
        std::atomic<Poco::Logger *> log;
    };

protected:
    String host;
    UInt64 port;
    String remote_replica;
    String remote_endpoint;
    String replica_name;

    String user;
    String password;

    Poco::Net::SocketAddress current_resolved_address;

    String description;
    void setDescription();

    bool connected{false};
    String server_name;
    UInt64 server_version_major{0};
    UInt64 server_version_minor{0};
    UInt64 server_revision{0};

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    ConnectionTimeouts timeouts;
    Poco::Timespan sync_request_timeout;

    LoggerWrapper log_wrapper;
};

} // end of namespace DB
