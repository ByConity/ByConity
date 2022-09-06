#include <Transaction/ICnchTransaction.h>

#include <Catalog/DataModelPartWrapper.h>
// #include <MergeTreeCommon/CnchPartsHelper.h>
// #include <MergeTreeCommon/CnchServerClient.h>
// #include <MergeTreeCommon/CnchServerClientPool.h>
#include <ResourceGroup/IResourceGroupManager.h>
#include <Transaction/LockManager.h>
#include <cppkafka/topic_partition_list.h>
#include <Common/serverLocality.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
}

bool isReadOnlyTransaction(const DB::IAST * ast)
{
    ResourceSelectCase::QueryType query_type = ResourceSelectCase::getQueryType(ast);
    return query_type == ResourceSelectCase::QueryType::SELECT || query_type == ResourceSelectCase::QueryType::OTHER;
}

CnchTransactionStatus ICnchTransaction::getStatus() const
{
    auto lock = getLock();
    return txn_record.status();
}

void ICnchTransaction::setStatus(CnchTransactionStatus status)
{
    auto lock = getLock();
    txn_record.setStatus(status);
}

void ICnchTransaction::setTransactionRecord(TransactionRecord record)
{
    auto lock = getLock();
    txn_record = std::move(record);
}

// IntentLockPtr ICnchTransaction::createIntentLock(const LockEntity & entity, const Strings & intent_names)
// {
//     return std::make_unique<IntentLock>(context, getTransactionRecord(), entity, intent_names);
// }

// void ICnchTransaction::lock(LockInfoPtr lockInfo)
// {
//     if (!tryLock(std::move(lockInfo))) {
//         throw Exception("Unable to acquire the lock", ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED);
//     }
// }

// bool ICnchTransaction::tryLock(LockInfoPtr lockInfo)
// {
//     if (!context.getConfigRef().getBool("enable_lock_manager", true))
//     {
//         throw Exception("Lock manager is disabled", ErrorCodes::SUPPORT_IS_DISABLED);
//     }

//     lockInfo->setTxnID(getTransactionID());
//     auto cnch_lock = std::make_unique<CnchLock>(std::move(lockInfo));
//     bool locked = cnch_lock->tryLock(context);
//     if (locked)
//     {
//         {
//             std::lock_guard lock(cnch_lock_mutex);
//             cnch_locks.push_back(std::move(cnch_lock));
//         }

//         if (!report_lock_heartbeat_task.hasTask())
//         {
//             report_lock_heartbeat_task
//                 = context.getSchedulePool().createTask("reportLockHeartBeat", [this]() { reportLockHeartBeatTask(); });
//             report_lock_heartbeat_task->activateAndSchedule();
//         }
//     }

//     return locked;
// }

void ICnchTransaction::unlock()
{
    // TODO: potential risk of deadlock here
    // {
    //     std::lock_guard lock(cnch_lock_mutex);
    //     for (const auto & cnch_lock : cnch_locks)
    //     {
    //         cnch_lock->unlock();
    //     }
    // }
}

// void ICnchTransaction::reportLockHeartBeat()
// {
//     NameSet host_ports;
//     {
//         std::lock_guard lock(cnch_lock_mutex);
//         for (const auto & cnch_lock : cnch_locks)
//         {
//             if (cnch_lock->isLocked())
//                 host_ports.emplace(cnch_lock->getHostPort());
//         }
//     }

//     auto local_host_port = std::to_string(context.getRPCPort());
//     for (const auto & host_port : host_ports)
//     {
//         if (isLocalServer(host_port, local_host_port))
//             LockManager::instance().updateExpireTime(getTransactionID(), LockManager::Clock::now() + lock_expire_duration);
//         else
//         {
//             auto client = context.getCnchServerClientPool().get(host_port);
//             client->reportCnchLockHeartBeat(getTransactionID(), lock_expire_duration.count());
//         }
//     }
// }

// void ICnchTransaction::reportLockHeartBeatTask()
// {
//     try
//     {
//         reportLockHeartBeat();
//         report_lock_heartbeat_task->scheduleAfter(heartbeat_interval);
//     }
//     catch (...)
//     {
//         tryLogCurrentException(log, __PRETTY_FUNCTION__);
//         report_lock_heartbeat_task->scheduleAfter(heartbeat_interval);
//     }
// }

void ICnchTransaction::setKafkaTpl(const String & consumer_group_, const cppkafka::TopicPartitionList & tpl_)
{
    this->consumer_group = consumer_group_;
    this->tpl = tpl_;
}

void ICnchTransaction::getKafkaTpl(String & consumer_group_, cppkafka::TopicPartitionList & tpl_) const
{
    consumer_group_ = this->consumer_group;
    tpl_ = this->tpl;
}

}
