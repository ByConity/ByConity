#include <Interpreters/HaReplicaHandler.h>

#include <Core/Protocol.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_INTERSERVER_IO_ENDPOINT;
    extern const int NO_SUCH_INTERSERVER_IO_ENDPOINT;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}


void HaDefaultReplicaEndpoint::processPacket(UInt64 packet_type, ReadBuffer &, WriteBuffer & out)
{
    using namespace DB::Protocol;
    switch (packet_type)
    {
        case HaClient::Hello:
            throw Exception("Unexpected packet " + String(HaClient::toString(packet_type)) + " received from client",
                    ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

        case HaClient::Ping:
            writeVarUInt(HaServer::Pong, out);
            out.next();
            return;

        default:
            throw Exception("Unknown packet " + toString(packet_type) + " from client",
                    ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    }
}

void HaReplicaHandler::addEndpoint(const String & name, HaReplicaEndpointPtr endpoint)
{
    std::lock_guard<std::mutex> lock(mutex);
    bool inserted = endpoint_map.try_emplace(name, std::move(endpoint)).second;
    if (!inserted)
        throw Exception("Duplicate interserver IO endpoint: " + name, ErrorCodes::DUPLICATE_INTERSERVER_IO_ENDPOINT);
}

void HaReplicaHandler::removeEndpoint(const String & name)
{
    std::lock_guard<std::mutex> lock(mutex);
    if (!endpoint_map.erase(name))
        throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
}

HaReplicaEndpointPtr HaReplicaHandler::getEndpoint(const String & name)
{
    try
    {
        std::lock_guard<std::mutex> lock(mutex);
        return endpoint_map.at(name);
    }
    catch (...)
    {
        throw Exception("No interserver IO endpoint named " + name, ErrorCodes::NO_SUCH_INTERSERVER_IO_ENDPOINT);
    }
}

HaReplicaEndpointHolder::~HaReplicaEndpointHolder()
{
    try
    {
        endpoint->cancel();
        handler.removeEndpoint(name);
    }
    catch (...)
    {
        tryLogCurrentException("~HaReplicaEndpointHolder");
    }
}

}
