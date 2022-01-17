#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/ConcurrentShardMap.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <boost/noncopyable.hpp>
#include <bthread/mtx_cv_base.h>
#include <Poco/Logger.h>
#include <common/types.h>

namespace DB
{
class BroadcastSenderProxy;
using BroadcastSenderProxyPtr = std::shared_ptr<BroadcastSenderProxy>;
using BroadcastSenderProxyPtrs = std::vector<BroadcastSenderProxyPtr>;

class BroadcastSenderProxyRegistry final : private boost::noncopyable
{
public:
    static BroadcastSenderProxyRegistry & instance()
    {
        static BroadcastSenderProxyRegistry instance;
        return instance;
    }

    BroadcastSenderProxyPtr getOrCreate(DataTransKeyPtr data_key);

    void remove(DataTransKeyPtr data_key);

    size_t countProxies();

private:
    BroadcastSenderProxyRegistry();
    mutable bthread::Mutex mutex;
    using BroadcastSenderProxyEntry = std::weak_ptr<BroadcastSenderProxy>;
    std::unordered_map<String, BroadcastSenderProxyEntry> proxies;
    Poco::Logger * logger;
};

}
