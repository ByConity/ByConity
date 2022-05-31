// #include "PartLock.h"

// #include <Catalog/Catalog.h>

// namespace DB
// {

// PartLock::PartLock(
//     const Context & context_,
//     const std::shared_ptr<zkutil::ZooKeeper> & zookeeper,
//     const std::string & session_prefix_,
//     const std::string & session_id_,
//     const TxnTimestamp & txn_id_,
//     const StoragePtr & table_,
//     const std::vector<String> & partitions_or_parts_,
//     const std::string & session_message_)
//     :
//     context(context_),
//     session_id(session_id_),
//     txn_id(txn_id_),
//     table(table_),
//     partitions_or_parts(partitions_or_parts_)
// {
//     zookeeper_holder = std::make_shared<zkutil::ZooKeeperHolder>();
//     zookeeper_holder->initFromInstance(zookeeper);
//     lock = std::make_unique<zkutil::Lock>(std::move(zookeeper_holder), session_prefix_, session_id_, session_message_);

//     session_id_prefix = session_prefix_ + "/" + session_id_;
// }

// bool PartLock::checkLockLivess() const
// {
//     zkutil::Lock::Status lock_status = lock->tryCheck();
//     if (lock_status == zkutil::Lock::LOCKED_BY_ME)
//         return true;
//     else
//         return false;
// }

// bool PartLock::tryLock()
// {
//     bool session_locked = lock->tryLock();
//     if (!session_locked)
//         return false;

//     status = Status::LOCK_SESSION_CREATED;

//     bool part_locked;
//     bool part_reset = false;
//     std::map<String, std::vector<String>> conflict_parts;
//     part_locked = tryLockPartInKV(table, partitions_or_parts, conflict_parts);
//     if (!part_locked)
//     {
//         /// locked failed because there is conflict
//         if (conflict_parts.size() > 0)
//         {
//             for (const auto & i : conflict_parts)
//             {
//                 if (checkSessionAlive(i.first)) /// conflict
//                 {
//                     /// clear session in zk
//                     unLock();
//                     return false;
//                 }
//             }

//             /// the sessions for the expired parts are all expired, try clear the expired part lock in kv
//             part_reset = tryResetAndLockConflictPartsInKv(table, conflict_parts);
//         }
//         else
//         {
//             /// failed for other reason.
//             unLock();
//             return false;
//         }

//     }

//     if (part_locked || part_reset)
//     {
//         status = Status::LOCKED;
//         return true;
//     }
//     else
//     {
//         unLock();
//         return false;
//     }

// }

// void PartLock::unLock()
// {
//     if (status == Status::LOCKED)
//     {
//         unlockPartInKV(table, partitions_or_parts);
//         lock->unlock();
//         status = Status::UNLOCKED;
//     }
//     else if (status == Status::LOCK_SESSION_CREATED)
//     {
//         lock->unlock();
//         status = Status::UNLOCKED;
//     }
// }

// bool PartLock::checkSessionAlive(DB::String session_id_)
// {
//     auto zookeeper = zookeeper_holder->getZooKeeper();
//     Coordination::Stat stat;
//     std::string dummy;
//     bool result = zookeeper->tryGet(session_id_prefix + "/" + session_id_, dummy, &stat);
//     if (!result)
//         return true;
//     else
//         return false;
// }

// bool PartLock::tryLockPartInKV(const StoragePtr & table, const std::vector<String> & parts, std::map<String, std::vector<String>> & conflict_parts)
// {
//     /// TODO: need to insert the part names into kv
//     try
//     {
//         context.getCnchCatalog()->tryLockPartInKV(table, parts, conflict_parts, txn_id);
//     }
//     catch (...)
//     {
//         DB::tryLogCurrentException(__PRETTY_FUNCTION__);
//         return false;
//     }
//     return true;
// }

// void PartLock::unlockPartInKV(const StoragePtr & table, const std::vector<String> & parts)
// {
//     /// TODO: need to clear the part names into kv
//     try
//     {
//         context.getCnchCatalog()->unLockPartInKV(table, parts, txn_id);
//     }
//     catch (...)
//     {
//         DB::tryLogCurrentException(__PRETTY_FUNCTION__);
//     }
// }

// bool PartLock::tryResetAndLockConflictPartsInKv(const StoragePtr & table, const std::map<String, std::vector<String>> & conflict_parts)
// {
//     /// TODO: reset the expired kv
//     context.getCnchCatalog()->tryResetAndLockConflictPartsInKV(table, conflict_parts, txn_id);
//     return true;
// }

// }
