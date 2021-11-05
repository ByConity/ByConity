#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>

#include <map>
#include <memory>
#include <atomic>
#include <mutex>
#include <boost/noncopyable.hpp>

namespace DB
{

class ReadBuffer;
class WriteBuffer;


class IHaReplicaEndpoint : private boost::noncopyable
{
public:
    virtual ~IHaReplicaEndpoint() {}

    void cancel() { is_cancelled = true; }
    virtual bool isCancelled() { return is_cancelled; }

    virtual void processPacket(UInt64, ReadBuffer &, WriteBuffer &) = 0;

private:
    std::atomic<bool> is_cancelled {false};
};

using HaReplicaEndpointPtr = std::shared_ptr<IHaReplicaEndpoint>;


class HaDefaultReplicaEndpoint : public IHaReplicaEndpoint
{
public:
    void processPacket(UInt64, ReadBuffer & in, WriteBuffer & out) override;
};


class HaReplicaHandler
{
public:
    void addEndpoint(const String & name, HaReplicaEndpointPtr endpoint);
    void removeEndpoint(const String & name);
    HaReplicaEndpointPtr getEndpoint(const String & name);

private:
    using EndpointMap = std::map<String, HaReplicaEndpointPtr>;

    EndpointMap endpoint_map;
    std::mutex mutex;
};


class HaReplicaEndpointHolder
{
public:
    HaReplicaEndpointHolder(const String & name_, HaReplicaEndpointPtr endpoint_, HaReplicaHandler & handler_)
        : name(name_), endpoint(std::move(endpoint_)), handler(handler_)
    {
        handler.addEndpoint(name, endpoint);
    }

    ~HaReplicaEndpointHolder();

private:
    String name;
    HaReplicaEndpointPtr endpoint;
    HaReplicaHandler & handler;
};


}
