#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
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
    : data_key(std::move(data_key_)), wait_timeout_ms(5000), logger(&Poco::Logger::get("BroadcastSenderProxy"))
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
        waitBecomeRealSender(wait_timeout_ms);
    return real_sender->send(std::move(chunk));
}


BroadcastStatus BroadcastSenderProxy::finish(BroadcastStatusCode status_code, String message)
{
    if (!has_real_sender.load(std::memory_order_relaxed))
        waitBecomeRealSender(wait_timeout_ms);
    return real_sender->finish(status_code, message);
}

void BroadcastSenderProxy::merge(IBroadcastSender && sender)
{
    if (!has_real_sender.load(std::memory_order_relaxed))
        waitBecomeRealSender(wait_timeout_ms);

    BroadcastSenderProxy * other = dynamic_cast<BroadcastSenderProxy *>(&sender);
    if (!other)
        real_sender->merge(std::move(sender));
    else
    {
        if (!other->has_real_sender)
            throw Exception("Can't merge proxy has no real sender " + data_key->dump(), ErrorCodes::LOGICAL_ERROR);

        real_sender->merge(std::move(*other->real_sender));
        other->has_real_sender.store(false, std::memory_order_relaxed);
        
        std::unique_lock lock(other->mutex);
        other->context = ContextPtr();
        other->header = Block();
    }
}

String BroadcastSenderProxy::getName() const
{
    String prefix = "[Proxy]";
    return real_sender ? prefix + real_sender->getName() : prefix + data_key->dump();
}

void BroadcastSenderProxy::waitAccept(UInt32 timeout_ms)
{
    std::unique_lock lock(mutex);
    if (context)
        return;

    if (!wait_accept.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return this->header.operator bool(); }))
        throw Exception("Wait accept timeout for " + data_key->dump(), ErrorCodes::TIMEOUT_EXCEEDED);
}

void BroadcastSenderProxy::accept(ContextPtr context_, Block header_)
{
    std::unique_lock lock(mutex);
    if (header || context)
        throw Exception("Can't call accept twice for {} " + data_key->dump(), ErrorCodes::LOGICAL_ERROR);
    context = std::move(context_);
    header = std::move(header_);
    wait_timeout_ms = context->getSettingsRef().exchange_timeout_ms;
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

BroadcastSenderType BroadcastSenderProxy::getType()
{
    if (!has_real_sender.load(std::memory_order_relaxed))
        waitBecomeRealSender(wait_timeout_ms);
    return real_sender->getType();
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
