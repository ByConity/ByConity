// #include "SchemaRWLock.h"

// namespace DB
// {

// SchemaRWLock::SchemaRWLock(
//     const std::shared_ptr<zkutil::ZooKeeper> & zookeeper,
//     const std::string & lock_prefix_,
//     const std::string & lock_name_,
//     const std::string & lock_message_,
//     zkutil::LockType lock_type_)
// {
//     auto zookeeper_holder = std::make_shared<zkutil::ZooKeeperHolder>();
//     zookeeper_holder->initFromInstance(zookeeper);
//     lock = std::make_unique<zkutil::ReadWriteLock>(std::move(zookeeper_holder), lock_prefix_ + "/" + lock_name_, lock_type_, lock_message_);
// }

// bool SchemaRWLock::checkLockLivess() const
// {
//     zkutil::ReadWriteLock::Status lock_status = lock->tryCheck();
//     if (lock_status == zkutil::ReadWriteLock::LOCKED_BY_ME)
//         return true;
//     else
//         return false;
// }

// bool SchemaRWLock::tryLock()
// {
//     return lock->tryLock();
// }

// void SchemaRWLock::unLock()
// {
//     lock->unLock();
// }

// }


