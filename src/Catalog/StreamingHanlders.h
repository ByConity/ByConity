#pragma once
#include <Catalog/HandlerManager.h>
#include <Core/Types.h>
#include <Poco/Logger.h>
#include <brpc/stream.h>
#include <condition_variable>
#include <mutex>


namespace DB::Catalog
{

class StreamingHandlerBase : public std::enable_shared_from_this<StreamingHandlerBase>, public brpc::StreamInputHandler
{
public:
    using HandlerPtr = std::shared_ptr<StreamingHandlerBase>;
    using HandlerIterator = std::list<HandlerPtr>::iterator;

    StreamingHandlerBase(HandlerManager & manager_)
        : manager(manager_) {}

    virtual void on_idle_timeout(brpc::StreamId id) override;

    virtual void on_closed(brpc::StreamId) override;

    Poco::Logger * log = &Poco::Logger::get("StreamingHandler");
    HandlerManager & manager;
    HandlerIterator handler_it;
};

class ServerPartsHandler :  public StreamingHandlerBase
{

public:
    using GetPartsFunc = std::function<void(brpc::StreamId & sd, String & name_space, String & table_uuid, Strings & request_partitions, UInt64 & txnTimestamp)>;

    ServerPartsHandler(HandlerManager & manager_, const String & name_space_, const String & table_uuid_,
                       const Strings & partitions_, const UInt64 & txnTimestamp_, const GetPartsFunc & func_)
        : StreamingHandlerBase(manager_),
        name_space(name_space_),
        table_uuid(table_uuid_),
        partitions(partitions_),
        txnTimestamp(txnTimestamp_),
        func(func_) {}


    virtual int on_received_messages(brpc::StreamId id, butil::IOBuf *const *, size_t ) override;

private:
    String name_space;
    String table_uuid;
    Strings partitions;
    UInt64 txnTimestamp;
    GetPartsFunc func;
};

class ClientPartsHandler : public StreamingHandlerBase
{
public:
    using PartsLoadFunc = std::function<void(const String & part_meta)>;

    ClientPartsHandler(HandlerManager & manager_, const PartsLoadFunc & loadFunc_) : StreamingHandlerBase(manager_), parts_loader(loadFunc_){}

    virtual int on_received_messages(brpc::StreamId , butil::IOBuf *const messages[], size_t size) override;

    virtual void on_closed(brpc::StreamId id) override;

    void waitingForGetParts();

    String last_exception;
private:
    void finishedGetParts();

    PartsLoadFunc parts_loader;
    std::mutex sync_mutex;
    std::condition_variable sync_cv;
};

}
