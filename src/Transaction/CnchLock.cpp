#include <Transaction/CnchLock.h>

#include <Core/UUID.h>
// #include <MergeTreeCommon/CnchServerClient.h>
// #include <MergeTreeCommon/CnchServerClientPool.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Transaction/LockDefines.h>
#include <Transaction/LockManager.h>
#include <Common/Exception.h>
#include <Common/serverLocality.h>
#include <Interpreters/Context.h>

#include <atomic>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
}

CnchLock::~CnchLock()
{
    try
    {
        if (!keep_alive)
            unlock();
    }
    catch (...)
    {
    }
}

bool CnchLock::tryLock(const Context & context)
{
    auto server = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(lock_info->table_uuid), false);
    lock_info->lock_id = context.getTimestamp();
    host_with_rpc = server.getRPCAddress();

    if (isLocalServer(host_with_rpc, std::to_string(context.getRPCPort())))
    {
        LockManager::instance().lock(lock_info, context);
    }
    else
    {
        // client = context.getCnchServerClientPool().get(host_with_rpc);
        // client->acquireLock(lock_info);
    }

    locked.store(lock_info->status == LockStatus::LOCK_OK, std::memory_order_release);
    return isLocked();
}

void CnchLock::lock(const Context & context)
{
    if (!tryLock(context))
    {
        throw Exception("Unable to acquire the lock", ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED);
    }
}

void CnchLock::unlock()
{
    if (isLocked())
    {
        // if (client)
        //     client->releaseLock(lock_info);
        // else
        LockManager::instance().unlock(lock_info);

        locked.store(false, std::memory_order_release);
    }
}

bool CnchLock::isLocked()
{
    return locked.load(std::memory_order_acquire);
}

}
