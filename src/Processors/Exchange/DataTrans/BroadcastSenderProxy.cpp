#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

BroadcastSenderProxy::BroadcastSenderProxy(DataTransKeyPtr data_key_)
    : data_key(std::move(data_key_)), logger(&Poco::Logger::get("BroadcastSenderProxy"))
{
}

BroadcastSenderProxy::~BroadcastSenderProxy()
{
    try
    {
        BroadcastSenderProxyRegistry::instance().remove(data_key);
    }
    catch (...)
    {
        tryLogCurrentException(logger);
    }
}


BroadcastStatus BroadcastSenderProxy::send(Chunk chunk)
{
    if (!has_real_sender.load(std::memory_order_relaxed))
        waitBecomeRealSender(5000);
    return real_sender->send(std::move(chunk));
}


BroadcastStatus BroadcastSenderProxy::finish(BroadcastStatusCode status_code, String message)
{
    if (!has_real_sender.load(std::memory_order_relaxed))
        return BroadcastStatus(BroadcastStatusCode::SEND_NOT_READY);
    return real_sender->finish(status_code, message);
}

void BroadcastSenderProxy::merge(IBroadcastSender && sender)
{
    real_sender->merge(std::move(sender));
}

String BroadcastSenderProxy::getName() const
{
    String prefix = "Proxy for: ";
    return real_sender ? prefix + real_sender->getName() : prefix + data_key->dump();
}

void BroadcastSenderProxy::waitAccept(UInt32 timeout_ms)
{
    std::unique_lock lock(mutex);
    if (context)
        return;

    if (!wait_accept.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return this->header.operator bool(); }))
        throw Exception("Wait accept timeout for {} " + data_key->dump(), ErrorCodes::TIMEOUT_EXCEEDED);
}

void BroadcastSenderProxy::accept(ContextPtr context_, Block header_)
{
    std::unique_lock lock(mutex);
    if (header || context)
        throw Exception("Can't call accept twice for {} " + data_key->dump(), ErrorCodes::LOGICAL_ERROR);
    context = std::move(context_);
    header = std::move(header_);
    wait_accept.notify_all();
}

void BroadcastSenderProxy::becomeRealSender(BroadcastSenderPtr sender)
{
    std::lock_guard lock(mutex);
    if (real_sender)
    {
        if (real_sender != sender)
            throw Exception("Can't set set real sender twice for " + data_key->dump(), ErrorCodes::LOGICAL_ERROR);
        return;
    }

    LOG_DEBUG(logger, "Proxy become real sender: {}", sender->getName());
    real_sender = std::move(sender);
    has_real_sender.store(true, std::memory_order_relaxed);
    wait_become_real.notify_all();
}

void BroadcastSenderProxy::waitBecomeRealSender(UInt32 timeout_ms)
{
    std::unique_lock lock(mutex);
    if (real_sender)
        return;
    if (!wait_become_real.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return this->real_sender.operator bool(); }))
        throw Exception("Wait become real sender timeout for {} " + data_key->dump(), ErrorCodes::TIMEOUT_EXCEEDED);
}

ContextPtr BroadcastSenderProxy::getContext() const
{
    std::lock_guard lock(mutex);
    return context;
}

Block BroadcastSenderProxy::getHeader() const
{
    std::lock_guard lock(mutex);
    return header;
}

}
