#pragma once

#include <Interpreters/StorageID.h>
#include <Transaction/LockRequest.h>

namespace DB
{
class Context;
class CnchServerClient;
using CnchServerClientPtr = std::shared_ptr<CnchServerClient>;

class CnchLock
{
public:
    explicit CnchLock(LockInfoPtr info)
        : lock_info(std::move(info))
    {
    }

    ~CnchLock();
    CnchLock(const CnchLock &) = delete;
    CnchLock & operator=(const CnchLock &) = delete;

    const String & getHostPort() { return host_with_rpc; }

    bool tryLock(const Context & context);
    void lock(const Context & context);
    void unlock();
    bool isLocked();

    // the lock will not be released when CnchLock object gets destroied
    void setKeepAlive(bool alive) { keep_alive = alive; }

private:
    bool keep_alive{false};
    std::atomic<bool> locked{false};
    LockInfoPtr lock_info;
    String host_with_rpc;
    // CnchServerClientPtr client;
};

using CnchLockPtr = std::unique_ptr<CnchLock>;
using CnchLockPtrs = std::vector<CnchLockPtr>;
}
