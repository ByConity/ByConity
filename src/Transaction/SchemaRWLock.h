// #pragma once

// #include <Common/ZooKeeper/ReadWriteLock.h>
// #include <Common/ZooKeeper/ZooKeeperHolder.h>

// namespace DB
// {
// /*
//  *  This class uses the ZooKeeper::waitForDisappear implementation, if the node doesn't exist, the watch will leak.
//  *  This issue also appears on http://zookeeper-user.578899.n2.nabble.com/Memory-leak-caused-by-a-watch-that-will-never-be-triggered-td7577498.html.
//  *  Can solve with getData instead of exists. Before it is solved, we better to use SchemaLock which uses the non rw zk lock implementation.
//  */
// class SchemaRWLock
// {
// public:
//     SchemaRWLock(
//         const std::shared_ptr<zkutil::ZooKeeper> & zookeeper,
//         const std::string & lock_prefix_,
//         const std::string & lock_name_,
//         const std::string & lock_message_ = "",
//         zkutil::LockType lock_type_ = zkutil::LockType::READ_LOCK);

//     SchemaRWLock(const SchemaRWLock &) = delete;
//     SchemaRWLock(SchemaRWLock && lock) = default;
//     SchemaRWLock & operator=(const SchemaRWLock &) = delete;

//     ~SchemaRWLock()
//     {
//         try
//         {
//             unLock();
//         }
//         catch (...)
//         {
//             DB::tryLogCurrentException(__PRETTY_FUNCTION__);
//         }
//     }

//     /// check lock liveness and still locked by me, is used to validate the session liveness after the lock is locked.
//     bool checkLockLivess() const;

//     bool tryLock();
//     void unLock();


// private:
//     std::unique_ptr<zkutil::ReadWriteLock> lock;
// };

// }




