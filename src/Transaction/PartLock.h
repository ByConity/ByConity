// #pragma once

// #include <Common/ZooKeeper/Lock.h>
// #include <Common/ZooKeeper/ZooKeeperHolder.h>
// #include <Interpreters/Context.h>
// #include <Transaction/TxnTimestamp.h>

// namespace DB
// {

// class PartLock
// {
// public:
//     PartLock(
//         const Context & context_,
//         const std::shared_ptr<zkutil::ZooKeeper> & zookeeper,
//         const std::string & session_prefix_,
//         const std::string & session_id_,
//         const TxnTimestamp & txn_id_,
//         const StoragePtr & table_,
//         const std::vector<String> & partitions_or_parts_,
//         const std::string & session_message_ = "");

//     ~PartLock()
//     {
//         partitions_or_parts.clear();

//         try
//         {
//             unLock();
//         }
//         catch (...)
//         {
//             DB::tryLogCurrentException(__PRETTY_FUNCTION__);
//         }
//     }

//     enum Status
//     {
//         UNLOCKED,
//         LOCK_SESSION_CREATED,
//         LOCKED,
//     };

//     /// check lock liveness and still locked by me, is used to validate the session liveness after the lock is locked.
//     bool checkLockLivess() const;

//     bool tryLock();
//     void unLock();

// private:
//     const Context & context;
//     zkutil::ZooKeeperHolderPtr zookeeper_holder;
//     std::unique_ptr<zkutil::Lock> lock;
//     Status status = Status::UNLOCKED;

//     String session_id_prefix;
//     String session_id;
//     const TxnTimestamp & txn_id;

//     const StoragePtr table;
//     std::vector<String> partitions_or_parts; /// can be partition list or part list.

//     /// check session liveness
//     bool checkSessionAlive(String session_id_);

//     /// CAS in bytekv, put if not exist, key is part name, value is session id
//     bool tryLockPartInKV(const StoragePtr & table, const std::vector<String> & parts, std::map<String, std::vector<String>> & conflict_parts);

//     /// CAS in bytekv, remove if value equals session_id
//     void unlockPartInKV(const StoragePtr & table, const std::vector<String> & parts);

//     /// CAS API in bytekv, put if value equals conflicted session_id
//     bool tryResetAndLockConflictPartsInKv(const StoragePtr & table, const std::map<String, std::vector<String>> & conflict_parts);
// };

// }




