#include <Storages/Kafka/StorageReplicaComponent.h>
#include <Common/Macros.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
}

StorageReplicaComponent::StorageReplicaComponent(
        const String & zookeeper_path_,
        const String & replica_name_,
        bool attach_,
        const String & /* database_name_ */,
        const String & /* table_name_ */,
        ContextPtr context_)
    : WithContext(context_->getGlobalContext())
    , zookeeper_path(getContext()->getMacros()->expand(zookeeper_path_))
    , replica_name(getContext()->getMacros()->expand(replica_name_))
    ///, log(&Poco::Logger::get(database_name_ + "." + table_name_ + " (ReplicaComponent)"))
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;
    replica_path = zookeeper_path + "/replicas/" + replica_name;

    if (getContext()->hasZooKeeper())
        current_zookeeper = getContext()->getZooKeeper();

    if (!current_zookeeper)
    {
        if (!attach_)
            throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        /// Do not activate the replica. It will be readonly.
        /// LOG_ERROR(log, "No ZooKeeper: table will be in readonly mode.");
        is_readonly = true;
    }
}

StorageReplicaComponent::~StorageReplicaComponent()
{
}

void StorageReplicaComponent::getStatus(Status & res)
{
    auto zookeeper = tryGetZooKeeper();

    res.is_leader = is_leader;
    res.is_readonly = is_readonly;
    res.is_session_expired = !zookeeper || zookeeper->expired();

    res.zookeeper_path = zookeeper_path;
    res.replica_name = replica_name;
    res.replica_path = replica_path;
}

zkutil::ZooKeeperPtr StorageReplicaComponent::tryGetZooKeeper()
{
    std::lock_guard<std::mutex> lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr StorageReplicaComponent::getZooKeeper()
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    return res;
}

void StorageReplicaComponent::setZooKeeper(zkutil::ZooKeeperPtr zookeeper)
{
    std::lock_guard<std::mutex> lock(current_zookeeper_mutex);
    current_zookeeper = zookeeper;
}

} // end of namespace DB
