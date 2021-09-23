#include <Storages/StorageHaMergeTree.h>

#include <Storages/MergeTree/HaMergeTreeReplicaEndpoint.h>
#include <Storages/MergeTree/HaMergeTreeBlockOutputStream.h>
#include <IO/ConnectionTimeoutsContext.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_ZOOKEEPER;
    extern const int INCORRECT_DATA;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int REPLICA_IS_ALREADY_EXIST;
    extern const int NO_REPLICA_HAS_PART;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
    extern const int ABORTED;
    extern const int REPLICA_IS_NOT_IN_QUORUM;
    extern const int TABLE_IS_READ_ONLY;
    extern const int NOT_FOUND_NODE;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int NOT_A_LEADER;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int PARTITION_ALREADY_EXISTS;
    extern const int TOO_MANY_RETRIES_TO_FETCH_PARTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int PARTITION_DOESNT_EXIST;
    extern const int UNFINISHED;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int TOO_MANY_FETCHES;
    extern const int BAD_DATA_PART_NAME;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int KEEPER_EXCEPTION;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_STATUS_CHANGED;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_POLICY;
    extern const int NO_SUCH_DATA_PART;
    extern const int INTERSERVER_SCHEME_DOESNT_MATCH;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsFetch;
    extern const StorageActionBlockType PartsSend;
    extern const StorageActionBlockType ReplicationQueue;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
}

[[maybe_unused]] static const auto QUEUE_UPDATE_ERROR_SLEEP_MS        = 1 * 1000;
[[maybe_unused]] static const auto MERGE_SELECTING_SLEEP_MS           = 5 * 1000;
[[maybe_unused]] static const auto MUTATIONS_FINALIZING_SLEEP_MS      = 1 * 1000;
[[maybe_unused]] static const auto MUTATIONS_FINALIZING_IDLE_SLEEP_MS = 5 * 1000;

void StorageHaMergeTree::setZooKeeper()
{
    /// Every HaMergeTree table is using only one ZooKeeper session.
    /// But if several HaMergeTree tables are using different
    /// ZooKeeper sessions, some queries like ATTACH PARTITION FROM may have
    /// strange effects. So we always use only one session for all tables.
    /// (excluding auxiliary zookeepers)

    std::lock_guard lock(current_zookeeper_mutex);
    if (zookeeper_name == default_zookeeper_name)
    {
        current_zookeeper = global_context.getZooKeeper();
    }
    else
    {
        current_zookeeper = global_context.getAuxiliaryZooKeeper(zookeeper_name);
    }
}

zkutil::ZooKeeperPtr StorageHaMergeTree::tryGetZooKeeper() const
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr StorageHaMergeTree::getZooKeeper() const
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    return res;
}

static std::string normalizeZooKeeperPath(std::string zookeeper_path)
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    return zookeeper_path;
}

static String extractZooKeeperName(const String & path)
{
    if (path.empty())
        throw Exception("ZooKeeper path should not be empty", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    auto pos = path.find(':');
    if (pos != String::npos)
    {
        auto zookeeper_name = path.substr(0, pos);
        if (zookeeper_name.empty())
            throw Exception("Zookeeper path should start with '/' or '<auxiliary_zookeeper_name>:/'", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return zookeeper_name;
    }
    static constexpr auto default_zookeeper_name = "default";
    return default_zookeeper_name;
}

static String extractZooKeeperPath(const String & path)
{
    if (path.empty())
        throw Exception("ZooKeeper path should not be empty", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    auto pos = path.find(':');
    if (pos != String::npos)
    {
        return normalizeZooKeeperPath(path.substr(pos + 1, String::npos));
    }
    return normalizeZooKeeperPath(path);
}

StorageHaMergeTree::StorageHaMergeTree(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    Context & context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    bool has_force_restore_data_flag,
    bool allow_renaming_)
    : MergeTreeData(
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name,
        merging_params_,
        std::move(settings_),
        true, /// require_part_metadata
        attach,
        [this](const std::string & name) { enqueuePartForCheck(name); })
    , zookeeper_name(extractZooKeeperName(zookeeper_path_))
    , zookeeper_path(extractZooKeeperPath(zookeeper_path_))
    , replica_name(replica_name_)
    , replica_path(zookeeper_path + "/replicas/" + replica_name_)
    , reader(*this)
    , writer(*this)
    , merger_mutator(*this, global_context.getSettingsRef().background_pool_size)
    /// , merge_strategy_picker(*this)
    , log_exchanger(*this)
    , queue(*this)
    , fetcher(*this)
    , background_executor(*this, global_context)
    , background_moves_executor(*this, global_context)
    // , cleanup_thread(*this)
    // , part_check_thread(*this)
    , restarting_thread(*this)
    , allow_renaming(allow_renaming_)
    , replicated_fetches_pool_size(global_context.getSettingsRef().background_fetches_pool_size)
{
    auto data_paths = getDataPaths();

    /// TODO: check data_paths
    log_manager = std::make_unique<HaMergeTreeLogManager>(
        data_paths.front() + "/log", getStorageID().getFullTableName() + " (HaLogManager)", !attach, *this);

    queue_updating_task = global_context.getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageHaMergeTree::queueUpdatingTask)", [this] { queueUpdatingTask(); });

    mutations_updating_task = global_context.getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageHaMergeTree::mutationsUpdatingTask)", [this] { mutationsUpdatingTask(); });

    merge_selecting_task = global_context.getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageHaMergeTree::mergeSelectingTask)", [this] { mergeSelectingTask(); });

    /// Will be activated if we win leader election.
    merge_selecting_task->deactivate();

    mutations_finalizing_task = global_context.getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageHaMergeTree::mutationsFinalizingTask)", [this] { mutationsFinalizingTask(); });

    if (global_context.hasZooKeeper() || global_context.hasAuxiliaryZooKeeper(zookeeper_name))
    {
        /// It's possible for getZooKeeper() to timeout if  zookeeper host(s) can't
        /// be reached. In such cases Poco::Exception is thrown after a connection
        /// timeout - refer to src/Common/ZooKeeper/ZooKeeperImpl.cpp:866 for more info.
        ///
        /// Side effect of this is that the CreateQuery gets interrupted and it exits.
        /// But the data Directories for the tables being created aren't cleaned up.
        /// This unclean state will hinder table creation on any retries and will
        /// complain that the Directory for table already exists.
        ///
        /// To achieve a clean state on failed table creations, catch this error and
        /// call dropIfEmpty() method only if the operation isn't ATTACH then proceed
        /// throwing the exception. Without this, the Directory for the tables need
        /// to be manually deleted before retrying the CreateQuery.
        try
        {
            if (zookeeper_name == default_zookeeper_name)
            {
                current_zookeeper = global_context.getZooKeeper();
            }
            else
            {
                current_zookeeper = global_context.getAuxiliaryZooKeeper(zookeeper_name);
            }
        }
        catch (...)
        {
            if (!attach)
                dropIfEmpty();
            throw;
        }
    }

    bool skip_sanity_checks = false;

    if (current_zookeeper && current_zookeeper->exists(replica_path + "/flags/force_restore_data"))
    {
        skip_sanity_checks = true;
        current_zookeeper->remove(replica_path + "/flags/force_restore_data");

        LOG_WARNING(
            log, "Skipping the limits on severity of changes to data parts and columns (flag {}/flags/force_restore_data).", replica_path);
    }
    else if (has_force_restore_data_flag)
    {
        skip_sanity_checks = true;

        LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag force_restore_data).");
    }

    loadDataParts(skip_sanity_checks);

    if (!current_zookeeper)
    {
        if (!attach)
        {
            dropIfEmpty();
            throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
        }

        /// Do not activate the replica. It will be readonly.
        LOG_ERROR(log, "No ZooKeeper: table will be in readonly mode.");
        is_readonly = true;
        return;
    }

    if (attach && !current_zookeeper->exists(zookeeper_path + "/metadata"))
    {
        LOG_WARNING(log, "No metadata in ZooKeeper: table will be in readonly mode.");
        is_readonly = true;
        has_metadata_in_zookeeper = false;
        return;
    }

    auto metadata_snapshot = getInMemoryMetadataPtr();

    if (!attach)
    {
        if (!getDataParts().empty())
            throw Exception(
                "Data directory for table already containing data parts"
                " - probably it was unclean DROP table or manual intervention."
                " You must either clear directory by hand or use ATTACH TABLE"
                " instead of CREATE TABLE if you need to use that parts.",
                ErrorCodes::INCORRECT_DATA);

        try
        {
            bool is_first_replica = createTableIfNotExists(metadata_snapshot);

            try
            {
                /// NOTE If it's the first replica, these requests to ZooKeeper look redundant, we already know everything.

                /// We have to check granularity on other replicas. If it's fixed we
                /// must create our new replica with fixed granularity and store this
                /// information in /replica/metadata.
                other_replicas_fixed_granularity = checkFixedGranualrityInZookeeper();

                checkTableStructure(zookeeper_path, metadata_snapshot);

                Coordination::Stat metadata_stat;
                current_zookeeper->get(zookeeper_path + "/metadata", &metadata_stat);
                metadata_version = metadata_stat.version;
            }
            catch (Coordination::Exception & e)
            {
                if (!is_first_replica && e.code == Coordination::Error::ZNONODE)
                    throw Exception("Table " + zookeeper_path + " was suddenly removed.", ErrorCodes::ALL_REPLICAS_LOST);
                else
                    throw;
            }

            if (!is_first_replica)
                createReplica(metadata_snapshot);
        }
        catch (...)
        {
            /// If replica was not created, rollback creation of data directory.
            dropIfEmpty();
            throw;
        }
    }
    else
    {
        /// In old tables this node may missing or be empty
        String replica_metadata;
        bool replica_metadata_exists = current_zookeeper->tryGet(replica_path + "/metadata", replica_metadata);
        if (!replica_metadata_exists || replica_metadata.empty())
        {
            /// We have to check shared node granularity before we create ours.
            other_replicas_fixed_granularity = checkFixedGranualrityInZookeeper();
            HaMergeTreeTableMetadata current_metadata(*this, metadata_snapshot);
            current_zookeeper->createOrUpdate(replica_path + "/metadata", current_metadata.toString(), zkutil::CreateMode::Persistent);
        }

        checkTableStructure(replica_path, metadata_snapshot);
        checkParts(skip_sanity_checks);

        if (current_zookeeper->exists(replica_path + "/metadata_version"))
        {
            metadata_version = parse<int>(current_zookeeper->get(replica_path + "/metadata_version"));
        }
        else
        {
            /// This replica was created with old clickhouse version, so we have
            /// to take version of global node. If somebody will alter our
            /// table, then we will fill /metadata_version node in zookeeper.
            /// Otherwise on the next restart we can again use version from
            /// shared metadata node because it was not changed.
            Coordination::Stat metadata_stat;
            current_zookeeper->get(zookeeper_path + "/metadata", &metadata_stat);
            metadata_version = metadata_stat.version;
        }
        /// Temporary directories contain untinalized results of Merges or Fetches (after forced restart)
        ///  and don't allow to reinitialize them, so delete each of them immediately
        clearOldTemporaryDirectories(0);
        clearOldWriteAheadLogs();
    }

    createNewZooKeeperNodes();
}

bool StorageHaMergeTree::checkFixedGranualrityInZookeeper()
{
    auto zookeeper = getZooKeeper();
    String metadata_str = zookeeper->get(zookeeper_path + "/metadata");
    auto metadata_from_zk = HaMergeTreeTableMetadata::parse(metadata_str);
    return metadata_from_zk.index_granularity_bytes == 0;
}

bool StorageHaMergeTree::createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot)
{
    auto zookeeper = getZooKeeper();
    zookeeper->createAncestors(zookeeper_path);

    for (size_t i = 0; i < 1000; ++i)
    {
        /// Invariant: "replicas" does not exist if there is no table or if there are leftovers from incompletely dropped table.
        if (zookeeper->exists(zookeeper_path + "/replicas"))
        {
            LOG_DEBUG(log, "This table {} is already created, will add new replica", zookeeper_path);
            return false;
        }

        /// There are leftovers from incompletely dropped table.
        if (zookeeper->exists(zookeeper_path + "/dropped"))
        {
            /// This condition may happen when the previous drop attempt was not completed
            ///  or when table is dropped by another replica right now.
            /// This is Ok because another replica is definitely going to drop the table.

            LOG_WARNING(log, "Removing leftovers from table {} (this might take several minutes)", zookeeper_path);
            String drop_lock_path = zookeeper_path + "/dropped/lock";
            Coordination::Error code = zookeeper->tryCreate(drop_lock_path, "", zkutil::CreateMode::Ephemeral);

            if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_WARNING(log, "The leftovers from table {} were removed by another replica", zookeeper_path);
            }
            else if (code != Coordination::Error::ZOK)
            {
                throw Coordination::Exception(code, drop_lock_path);
            }
            else
            {
                auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *zookeeper);
                if (!removeTableNodesFromZooKeeper(zookeeper, zookeeper_path, metadata_drop_lock, log))
                {
                    /// Someone is recursively removing table right now, we cannot create new table until old one is removed
                    continue;
                }
            }
        }

        LOG_DEBUG(log, "Creating table {}", zookeeper_path);

        /// We write metadata of table so that the replicas can check table parameters with them.
        String metadata_str = HaMergeTreeTableMetadata(*this, metadata_snapshot).toString();

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", metadata_str,
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/columns", metadata_snapshot->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/blocks", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/block_numbers", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/leader_election", "",
            zkutil::CreateMode::Persistent));
        /// TODO: do we need it ?
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/temp", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name,
            zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(getZKLatestLSNPath(), "0", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(getZKLatestLSNPath() + "/lsn-", "", zkutil::CreateMode::EphemeralSequential));
        ops.emplace_back(zkutil::makeCreateRequest(getZKCommittedLSNPath(), "0", zkutil::CreateMode::Persistent));

        /// And create first replica atomically. See also "createReplica" method that is used to create not the first replicas.

        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/flags", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/is_lost", "0",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata", metadata_str,
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/columns", metadata_snapshot->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata_version", std::to_string(metadata_version),
            zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(getZKReplicaUpdatedLSNPath(), "0", zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_WARNING(log, "It looks like the table {} was created by another server at the same moment, will retry", zookeeper_path);
            continue;
        }
        else if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }

        return true;
    }

    /// Do not use LOGICAL_ERROR code, because it may happen if user has specified wrong zookeeper_path
    throw Exception("Cannot create table, because it is created concurrently every time "
                    "or because of wrong zookeeper_path "
                    "or because of logical error", ErrorCodes::REPLICA_IS_ALREADY_EXIST);
}

void StorageHaMergeTree::createReplica(const StorageMetadataPtr & metadata_snapshot)
{
    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Creating replica {}", replica_path);

    Coordination::Error code;

    do
    {
        Coordination::Stat replicas_stat;
        String replicas_value;

        if (!zookeeper->tryGet(zookeeper_path + "/replicas", replicas_value, &replicas_stat))
            throw Exception(fmt::format("Cannot create a replica of the table {}, because the last replica of the table was dropped right now",
                zookeeper_path), ErrorCodes::ALL_REPLICAS_LOST);

        /// It is not the first replica, we will mark it as "lost", to immediately repair (clone) from existing replica.
        /// By the way, it's possible that the replica will be first, if all previous replicas were removed concurrently.
        String is_lost_value = replicas_stat.numChildren ? "1" : "0";

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/flags", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/is_lost", is_lost_value,
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata", HaMergeTreeTableMetadata(*this, metadata_snapshot).toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/columns", metadata_snapshot->getColumns().toString(),
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata_version", std::to_string(metadata_version),
            zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(getZKReplicaUpdatedLSNPath(), "0", zkutil::CreateMode::Persistent));

        /// Check version of /replicas to see if there are any replicas created at the same moment of time.
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name, replicas_stat.version));

        Coordination::Responses responses;
        code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            throw Exception("Replica " + replica_path + " already exists.", ErrorCodes::REPLICA_IS_ALREADY_EXIST);
        }
        else if (code == Coordination::Error::ZBADVERSION)
        {
            LOG_ERROR(log, "Retrying createReplica(), because some other replicas were created at the same time");
        }
        else if (code == Coordination::Error::ZNONODE)
        {
            throw Exception("Table " + zookeeper_path + " was suddenly removed.", ErrorCodes::ALL_REPLICAS_LOST);
        }
        else
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }
    } while (code == Coordination::Error::ZBADVERSION);
}

void StorageHaMergeTree::createNewZooKeeperNodes()
{
    auto zookeeper = getZooKeeper();

    /// Working with quorum.
    zookeeper->createIfNotExists(zookeeper_path + "/quorum", String());
    zookeeper->createIfNotExists(zookeeper_path + "/quorum/parallel", String());
    zookeeper->createIfNotExists(zookeeper_path + "/quorum/last_part", String());
    zookeeper->createIfNotExists(zookeeper_path + "/quorum/failed_parts", String());

    /// Mutations
    zookeeper->createIfNotExists(zookeeper_path + "/mutations", String());
    zookeeper->createIfNotExists(replica_path + "/mutation_pointer", String());

    /// For ALTER PARTITION with multi-leaders
    /// TODO: zookeeper->createIfNotExists(zookeeper_path + "/alter_partition_version", String());
}

/** Verify that list of columns and table storage_settings_ptr match those specified in ZK (/metadata).
  * If not, throw an exception.
  */
void StorageHaMergeTree::checkTableStructure(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot)
{
    auto zookeeper = getZooKeeper();

    HaMergeTreeTableMetadata old_metadata(*this, metadata_snapshot);

    Coordination::Stat metadata_stat;
    String metadata_str = zookeeper->get(zookeeper_prefix + "/metadata", &metadata_stat);
    auto metadata_from_zk = HaMergeTreeTableMetadata::parse(metadata_str);
    old_metadata.checkEquals(metadata_from_zk, metadata_snapshot->getColumns(), global_context);

    Coordination::Stat columns_stat;
    auto columns_from_zk = ColumnsDescription::parse(zookeeper->get(zookeeper_prefix + "/columns", &columns_stat));

    const ColumnsDescription & old_columns = metadata_snapshot->getColumns();
    if (columns_from_zk != old_columns)
    {
        throw Exception("Table columns structure in ZooKeeper is different from local table structure", ErrorCodes::INCOMPATIBLE_COLUMNS);
    }
}

void StorageHaMergeTree::drop()
{
    /// There is also the case when user has configured ClickHouse to wrong ZooKeeper cluster
    /// or metadata of staled replica were removed manually,
    /// in this case, has_metadata_in_zookeeper = false, and we also permit to drop the table.

    if (has_metadata_in_zookeeper)
    {
        /// Table can be shut down, restarting thread is not active
        /// and calling StorageHaMergeTree::getZooKeeper()/getAuxiliaryZooKeeper() won't suffice.
        zkutil::ZooKeeperPtr zookeeper;
        if (zookeeper_name == default_zookeeper_name)
            zookeeper = global_context.getZooKeeper();
        else
            zookeeper = global_context.getAuxiliaryZooKeeper(zookeeper_name);

        /// If probably there is metadata in ZooKeeper, we don't allow to drop the table.
        if (!zookeeper)
            throw Exception("Can't drop readonly replicated table (need to drop data in ZooKeeper as well)", ErrorCodes::TABLE_IS_READ_ONLY);

        shutdown();
        dropReplica(zookeeper, zookeeper_path, replica_name, log);
    }

    dropAllData();
}

void StorageHaMergeTree::dropLogEntries()
{
    log_manager->drop();
}

void StorageHaMergeTree::dropReplica(zkutil::ZooKeeperPtr zookeeper, const String & zookeeper_path, const String & replica, Poco::Logger * logger)
{
    if (zookeeper->expired())
        throw Exception("Table was not dropped because ZooKeeper session has expired.", ErrorCodes::TABLE_WAS_NOT_DROPPED);

    auto remote_replica_path = zookeeper_path + "/replicas/" + replica;
    LOG_INFO(logger, "Removing replica {}, marking it as lost", remote_replica_path);
    /// Mark itself lost before removing, because the following recursive removal may fail
    /// and partially dropped replica may be considered as alive one (until someone will mark it lost)
    zookeeper->trySet(zookeeper_path + "/replicas/" + replica + "/is_lost", "1");
    /// It may left some garbage if replica_path subtree are concurrently modified
    zookeeper->tryRemoveRecursive(remote_replica_path);
    if (zookeeper->exists(remote_replica_path))
        LOG_ERROR(logger, "Replica was not completely removed from ZooKeeper, {} still exists and may contain some garbage.", remote_replica_path);

    /// Check that `zookeeper_path` exists: it could have been deleted by another replica after execution of previous line.
    Strings replicas;
    if (Coordination::Error::ZOK != zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) || !replicas.empty())
        return;

    LOG_INFO(logger, "{} is the last replica, will remove table", remote_replica_path);

    /** At this moment, another replica can be created and we cannot remove the table.
      * Try to remove /replicas node first. If we successfully removed it,
      * it guarantees that we are the only replica that proceed to remove the table
      * and no new replicas can be created after that moment (it requires the existence of /replicas node).
      * and table cannot be recreated with new /replicas node on another servers while we are removing data,
      * because table creation is executed in single transaction that will conflict with remaining nodes.
      */

    /// Node /dropped works like a lock that protects from concurrent removal of old table and creation of new table.
    /// But recursive removal may fail in the middle of operation leaving some garbage in zookeeper_path, so
    /// we remove it on table creation if there is /dropped node. Creating thread may remove /dropped node created by
    /// removing thread, and it causes race condition if removing thread is not finished yet.
    /// To avoid this we also create ephemeral child before starting recursive removal.
    /// (The existence of child node does not allow to remove parent node).
    Coordination::Requests ops;
    Coordination::Responses responses;
    String drop_lock_path = zookeeper_path + "/dropped/lock";
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/replicas", -1));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/dropped", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(drop_lock_path, "", zkutil::CreateMode::Ephemeral));
    Coordination::Error code = zookeeper->tryMulti(ops, responses);

    if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
    {
        LOG_WARNING(logger, "Table {} is already started to be removing by another replica right now", remote_replica_path);
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_WARNING(logger, "Another replica was suddenly created, will keep the table {}", remote_replica_path);
    }
    else if (code != Coordination::Error::ZOK)
    {
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
    else
    {
        auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *zookeeper);
        LOG_INFO(logger, "Removing table {} (this might take several minutes)", zookeeper_path);
        removeTableNodesFromZooKeeper(zookeeper, zookeeper_path, metadata_drop_lock, logger);
    }
}

bool StorageHaMergeTree::removeTableNodesFromZooKeeper(zkutil::ZooKeeperPtr zookeeper,
        const String & zookeeper_path, const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock, Poco::Logger * logger)
{
    bool completely_removed = false;
    Strings children;
    Coordination::Error code = zookeeper->tryGetChildren(zookeeper_path, children);
    if (code == Coordination::Error::ZNONODE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of replicated table. It's a bug");


    for (const auto & child : children)
        if (child != "dropped")
            zookeeper->tryRemoveRecursive(zookeeper_path + "/" + child);

    Coordination::Requests ops;
    Coordination::Responses responses;
    ops.emplace_back(zkutil::makeRemoveRequest(metadata_drop_lock->getPath(), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/dropped", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path, -1));
    code = zookeeper->tryMulti(ops, responses);

    if (code == Coordination::Error::ZNONODE)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of replicated table. It's a bug");
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_ERROR(logger, "Table was not completely removed from ZooKeeper, {} still exists and may contain some garbage,"
                          "but someone is removing it right now.", zookeeper_path);
    }
    else if (code != Coordination::Error::ZOK)
    {
        /// It is still possible that ZooKeeper session is expired or server is killed in the middle of the delete operation.
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
    else
    {
        metadata_drop_lock->setAlreadyRemoved();
        completely_removed = true;
        LOG_INFO(logger, "Table {} was successfully removed from ZooKeeper", zookeeper_path);
    }

    return completely_removed;
}

void StorageHaMergeTree::checkParts(bool)
{
    /// TODO:
}

void StorageHaMergeTree::truncate(
    const ASTPtr &, const StorageMetadataPtr &, const Context & , TableExclusiveLockHolder & table_lock)
{
    table_lock.release();   /// Truncate is done asynchronously.

    assertNotReadonly();
    if (!is_leader)
        throw Exception("TRUNCATE cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    Strings partitions = zookeeper->getChildren(zookeeper_path + "/block_numbers");

    /*
    for (String & partition_id : partitions)
    {
        LogEntry entry;

        if (dropAllPartsInPartition(*zookeeper, partition_id, entry, query_context, false))
            waitForAllReplicasToProcessLogEntry(entry);
    }
    */
}

void StorageHaMergeTree::startup()
{
    if (is_readonly)
        return;

    try
    {
        queue.initialize(getDataParts());

        replica_endpoint_holder = std::make_unique<HaReplicaEndpointHolder>(
            replica_path, std::make_shared<HaMergeTreeReplicaEndpoint>(*this), global_context.getHaReplicaHandler());

        InterserverIOEndpointPtr data_parts_exchange_ptr = std::make_shared<DataPartsExchange::Service>(*this, shared_from_this());
        [[maybe_unused]] auto prev_ptr = std::atomic_exchange(&data_parts_exchange_endpoint, data_parts_exchange_ptr);
        assert(prev_ptr == nullptr);
        global_context.getInterserverIOHandler().addEndpoint(data_parts_exchange_ptr->getId(replica_path), data_parts_exchange_ptr);

        /// In this thread replica will be activated.
        restarting_thread.start();

        /// Wait while restarting_thread initializes LeaderElection (and so on) or makes first attempt to do it
        startup_event.wait();

        /// If we don't separate create/start steps, race condition will happen
        /// between the assignment of queue_task_handle and queueTask that use the queue_task_handle.
        background_executor.start();
        startBackgroundMovesIfNeeded();
    }
    catch (...)
    {
        /// Exception safety: failed "startup" does not require a call to "shutdown" from the caller.
        /// And it should be able to safely destroy table after exception in "startup" method.
        /// It means that failed "startup" must not create any background tasks that we will have to wait.
        try
        {
            shutdown();
        }
        catch (...)
        {
            std::terminate();
        }

        /// Note: after failed "startup", the table will be in a state that only allows to destroy the object.
        throw;
    }
}


void StorageHaMergeTree::shutdown()
{
    /// Cancel fetches, merges and mutations to force the queue_task to finish ASAP.
    fetcher.blocker.cancelForever();
    merger_mutator.merges_blocker.cancelForever();
    parts_mover.moves_blocker.cancelForever();

    restarting_thread.shutdown();
    background_executor.finish();

    /// TODO:
    /// {
    ///     auto lock = queue.lockQueue();
    ///     /// Cancel logs pulling after background task were cancelled. It's still
    ///     /// required because we can trigger pullLogsToQueue during manual OPTIMIZE,
    ///     /// MUTATE, etc. query.
    ///     queue.pull_log_blocker.cancelForever();
    /// }
    background_moves_executor.finish();

    auto data_parts_exchange_ptr = std::atomic_exchange(&data_parts_exchange_endpoint, InterserverIOEndpointPtr{});
    if (data_parts_exchange_ptr)
    {
        global_context.getInterserverIOHandler().removeEndpointIfExists(data_parts_exchange_ptr->getId(replica_path));
        /// Ask all parts exchange handlers to finish asap. New ones will fail to start
        data_parts_exchange_ptr->blocker.cancelForever();
        /// Wait for all of them
        std::unique_lock lock(data_parts_exchange_ptr->rwlock);
    }

    if (replica_endpoint_holder)
        replica_endpoint_holder = nullptr;
}

StorageHaMergeTree::~StorageHaMergeTree()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

HaMergeTreeQuorumAddedParts::PartitionIdToMaxBlock StorageHaMergeTree::getMaxAddedBlocks() const
{
    throw Exception("Not supported", ErrorCodes::SUPPORT_IS_DISABLED);
}

void StorageHaMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned num_streams)
{
    /** The `select_sequential_consistency` setting has two meanings:
    * 1. To throw an exception if on a replica there are not all parts which have been written down on quorum of remaining replicas.
    * 2. Do not read parts that have not yet been written to the quorum of the replicas.
    * For this you have to synchronously go to ZooKeeper.
    */
    if (context.getSettingsRef().select_sequential_consistency)
    {
        auto max_added_blocks = getMaxAddedBlocks();
        if (auto plan = reader.read(column_names, metadata_snapshot, query_info, context, max_block_size, num_streams, &max_added_blocks))
            query_plan = std::move(*plan);
        return;
    }

    if (auto plan = reader.read(column_names, metadata_snapshot, query_info, context, max_block_size, num_streams))
        query_plan = std::move(*plan);
}

Pipe StorageHaMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(QueryPlanOptimizationSettings(context.getSettingsRef()));
}


template <class Func>
void StorageHaMergeTree::foreachCommittedParts(Func && func, bool select_sequential_consistency) const
{
    std::optional<HaMergeTreeQuorumAddedParts::PartitionIdToMaxBlock> max_added_blocks = {};

    /**
     * Synchronously go to ZooKeeper when select_sequential_consistency enabled
     */
    if (select_sequential_consistency)
        max_added_blocks = getMaxAddedBlocks();

    auto lock = lockParts();
    for (const auto & part : getDataPartsStateRange(DataPartState::Committed))
    {
        if (part->isEmpty())
            continue;

        if (max_added_blocks)
        {
            auto blocks_iterator = max_added_blocks->find(part->info.partition_id);
            if (blocks_iterator == max_added_blocks->end() || part->info.max_block > blocks_iterator->second)
                continue;
        }

        func(part);
    }
}

std::optional<UInt64> StorageHaMergeTree::totalRows(const Settings & settings) const
{
    UInt64 res = 0;
    foreachCommittedParts([&res](auto & part) { res += part->rows_count; }, settings.select_sequential_consistency);
    return res;
}

std::optional<UInt64> StorageHaMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, const Context & context) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    PartitionPruner partition_pruner(metadata_snapshot->getPartitionKey(), query_info, context, true /* strict */);
    if (partition_pruner.isUseless())
        return {};
    size_t res = 0;
    foreachCommittedParts([&](auto & part)
    {
        if (!partition_pruner.canBePruned(part))
            res += part->rows_count;
    }, context.getSettingsRef().select_sequential_consistency);
    return res;
}

std::optional<UInt64> StorageHaMergeTree::totalBytes(const Settings & settings) const
{
    UInt64 res = 0;
    foreachCommittedParts([&res](auto & part) { res += part->getBytesOnDisk(); }, settings.select_sequential_consistency);
    return res;
}

void StorageHaMergeTree::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode (zookeeper path: {})", zookeeper_path);
}

HaMergeTreeAddress StorageHaMergeTree::getHaMergeTreeAddress() const
{
    auto host_port = global_context.getInterserverIOAddress();
    auto table_id = getStorageID();

    HaMergeTreeAddress res;
    res.host = host_port.first;
    res.replication_port = host_port.second;
    res.queries_port = global_context.getTCPPort();
    res.ha_port = global_context.getHaTCPPort();
    res.database = table_id.database_name;
    res.table = table_id.table_name;
    res.scheme = global_context.getInterserverScheme();
    return res;
}

HaMergeTreeAddress StorageHaMergeTree::getReplicaAddress(const String & replica_name_)
{
    String replica_host_path = zookeeper_path + "/replicas/" + replica_name_ + "/host";
    return HaMergeTreeAddress(getZooKeeper()->get(replica_host_path));
}

BlockOutputStreamPtr StorageHaMergeTree::write(const ASTPtr & /*query*/, [[maybe_unused]] const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    const auto storage_settings_ptr = getSettings();
    assertNotReadonly();

    const Settings & query_settings = context.getSettingsRef();
    [[maybe_unused]] bool deduplicate = storage_settings_ptr->replicated_deduplication_window != 0 && query_settings.insert_deduplicate;

    // TODO: should we also somehow pass list of columns to deduplicate on to the HaMergeTreeBlockOutputStream ?
    return std::make_shared<HaMergeTreeBlockOutputStream>(*this, metadata_snapshot, context);
}

bool StorageHaMergeTree::optimize(
    [[maybe_unused]]const ASTPtr &,
    [[maybe_unused]]const StorageMetadataPtr &,
    [[maybe_unused]]const ASTPtr & partition,
    [[maybe_unused]]bool final,
    [[maybe_unused]]bool deduplicate,
    [[maybe_unused]]const Names & deduplicate_by_columns,
    [[maybe_unused]]const Context & query_context)
{
    /// TODO:
    throw Exception("Not supported", ErrorCodes::SUPPORT_IS_DISABLED);
}

void StorageHaMergeTree::alter(
    [[maybe_unused]] const AlterCommands & commands,
    [[maybe_unused]] const Context & query_context,
    [[maybe_unused]] TableLockHolder & table_lock_holder)
{
    assertNotReadonly();
    throw Exception("Not supported", ErrorCodes::SUPPORT_IS_DISABLED);
}

void StorageHaMergeTree::mutate([[maybe_unused]] const MutationCommands & commands, [[maybe_unused]] const Context & query_context)
{
}

void StorageHaMergeTree::waitMutation(const String & , size_t ) const
{
}

std::vector<MergeTreeMutationStatus> StorageHaMergeTree::getMutationsStatus() const
{
    return {};
}

CancellationCode StorageHaMergeTree::killMutation([[maybe_unused]]const String & mutation_id)
{
    return {};
}


void StorageHaMergeTree::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    global_context.checkTableCanBeDropped(table_id.database_name, table_id.table_name, getTotalActiveSizeInBytes());
}

void StorageHaMergeTree::checkTableCanBeRenamed() const
{
    if (!allow_renaming)
        throw Exception("Cannot rename Ha table, because zookeeper_path contains implicit 'database' or 'table' macro. "
                        "We cannot rename path in ZooKeeper, so path may become inconsistent with table name. If you really want to rename table, "
                        "you should edit metadata file first and restart server or reattach the table.", ErrorCodes::NOT_IMPLEMENTED);
}

void StorageHaMergeTree::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    checkTableCanBeRenamed();
    MergeTreeData::rename(new_path_to_table_data, new_table_id);

    /// Update table name in zookeeper
    if (!is_readonly)
    {
        /// We don't do it for readonly tables, because it will be updated on next table startup.
        /// It is also Ok to skip ZK error for the same reason.
        try
        {
            auto zookeeper = getZooKeeper();
            /// TODO:
            /// zookeeper->set(replica_path + "/host", getHaMergeTreeAddress().toString());
        }
        catch (Coordination::Exception & e)
        {
            LOG_WARNING(log, "Cannot update the value of 'host' node (replica address) in ZooKeeper: {}", e.displayText());
        }
    }

    /// TODO: You can update names of loggers.
}

ActionLock StorageHaMergeTree::getActionLock(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return merger_mutator.merges_blocker.cancel();

    if (action_type == ActionLocks::PartsTTLMerge)
        return merger_mutator.ttl_merges_blocker.cancel();

    if (action_type == ActionLocks::PartsFetch)
        return fetcher.blocker.cancel();

    if (action_type == ActionLocks::PartsSend)
    {
        auto data_parts_exchange_ptr = std::atomic_load(&data_parts_exchange_endpoint);
        return data_parts_exchange_ptr ? data_parts_exchange_ptr->blocker.cancel() : ActionLock();
    }

    // if (action_type == ActionLocks::ReplicationQueue)
    //     return queue.actions_blocker.cancel();

    if (action_type == ActionLocks::PartsMove)
        return parts_mover.moves_blocker.cancel();

    return {};
}

void StorageHaMergeTree::onActionLockRemove(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge || action_type == ActionLocks::PartsTTLMerge
        || action_type == ActionLocks::PartsFetch || action_type == ActionLocks::PartsSend
        || action_type == ActionLocks::ReplicationQueue)
        background_executor.triggerTask();
    else if (action_type == ActionLocks::PartsMove)
        background_moves_executor.triggerTask();
}

void StorageHaMergeTree::getStatus(Status & res, bool with_zk_fields)
{
    auto zookeeper = getZooKeeper();

    res.is_leader = is_leader;
    res.can_become_leader = getSettings()->replicated_can_become_leader;
    res.is_readonly = is_readonly;
    res.is_session_expired = !zookeeper || zookeeper->expired();

    res.zookeeper_path = zookeeper_path;
    res.replica_name = replica_name;
    res.replica_path = replica_path;

    res.queue = queue.getStatus();

    res.absolute_delay = queue.getAbsoluteDelay().first;

    auto lsn_status = log_manager->getLSNStatus(true);
    res.committed_lsn = lsn_status.committed_lsn;
    res.updated_lsn = lsn_status.updated_lsn;

    if (res.is_session_expired || !with_zk_fields)
    {
        res.latest_lsn = 0;
        res.total_replicas = 0;
        res.active_replicas = 0;
    }
    else
    {
        auto all_replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
        res.total_replicas = all_replicas.size();

        res.active_replicas = 0;
        for (const String & replica : all_replicas)
        {
            if (zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
                ++res.active_replicas;
        }
        res.latest_lsn = parse<UInt64>(zookeeper->get(getZKLatestLSNPath()));
    }
}

void StorageHaMergeTree::getQueue(LogEntriesData & res, String & out_replica_name)
{
    out_replica_name = replica_name;
    queue.getUnprocessedEntries(res);
}

time_t StorageHaMergeTree::getAbsoluteDelay() const
{
    return queue.getAbsoluteDelay().first;
}

void StorageHaMergeTree::getReplicaDelays(time_t & out_absolute_delay, time_t & out_relative_delay)
{
    assertNotReadonly();

    out_absolute_delay = getAbsoluteDelay();
    out_relative_delay = 0;

    if (out_absolute_delay < static_cast<time_t>(getSettings()->min_relative_delay_to_yield_leadership))
        return;

    auto peer_delays = log_exchanger.getDelays();
    for (auto peer_delay : peer_delays)
    {
        if (peer_delay.second < out_absolute_delay)
            out_relative_delay = std::max(static_cast<Int64>(out_relative_delay),
                                          static_cast<Int64>(out_absolute_delay - peer_delay.second));
    }
}

CheckResults StorageHaMergeTree::checkData([[maybe_unused]]const ASTPtr & query, [[maybe_unused]]const Context & context)
{
    /*
    CheckResults results;
    DataPartsVector data_parts;
    if (const auto & check_query = query->as<ASTCheckQuery &>(); check_query.partition)
    {
        String partition_id = getPartitionIDFromQuery(check_query.partition, context);
        data_parts = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    }
    else
        data_parts = getDataPartsVector();

    for (auto & part : data_parts)
    {
        try
        {
            results.push_back(part_check_thread.checkPart(part->name));
        }
        catch (const Exception & ex)
        {
            results.emplace_back(part->name, false, "Check of part finished with error: '" + ex.message() + "'");
        }
    }
    return results;
    */
    return {};
}

bool StorageHaMergeTree::canUseAdaptiveGranularity() const
{
    const auto storage_settings_ptr = getSettings();
    return storage_settings_ptr->index_granularity_bytes != 0 &&
        (storage_settings_ptr->enable_mixed_granularity_parts ||
            (!has_non_adaptive_index_granularity_parts && !other_replicas_fixed_granularity));
}

std::optional<JobAndPool> StorageHaMergeTree::getDataProcessingJob()
{
    /// If replication queue is stopped exit immediately as we successfully executed the task
    /// if (queue.actions_blocker.isCancelled())
    ///     return {};

    /// This object will mark the element of the queue as running.
    /// ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry = selectQueueEntry();

    /// if (!selected_entry)
    ///     return {};

    /// PoolType pool_type;

    /// /// Depending on entry type execute in fetches (small) pool or big merge_mutate pool
    /// if (selected_entry->log_entry->type == LogEntry::GET_PART)
    ///     pool_type = PoolType::FETCH;
    /// else
    ///     pool_type = PoolType::MERGE_MUTATE;

    /// return JobAndPool{[this, selected_entry] () mutable
    /// {
    ///     return processQueueEntry(selected_entry);
    /// }, pool_type};
    return {};
}

bool StorageHaMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & ) const
{
    return false;
    /// TODO:
    /// return queue.isVirtualPart(part);
}

void StorageHaMergeTree::queueUpdatingTask()
{
}

void StorageHaMergeTree::mutationsUpdatingTask()
{
}

void StorageHaMergeTree::forceSetTableStructure(zkutil::ZooKeeperPtr & zookeeper)
{
    auto lock = lockExclusively(RWLockImpl::NO_QUERY, global_context.getSettingsRef().lock_acquire_timeout);

    auto metadata_snapshot = getInMemoryMetadataPtr();

    HaMergeTreeTableMetadata old_metadata(*this, metadata_snapshot);
    Coordination::Stat metadata_stat;
    auto zk_metadata_str = zookeeper->get(zookeeper_path + "/metadata", &metadata_stat);
    auto zk_metadata = HaMergeTreeTableMetadata::parse(zk_metadata_str);
    auto metadata_diff = old_metadata.checkAndFindDiff(zk_metadata);

    ColumnsDescription old_columns = metadata_snapshot->getColumns();
    auto zk_columns_str = zookeeper->get(zookeeper_path + "/columns");
    auto zk_columns = ColumnsDescription::parse(zk_columns_str);

    if (zk_columns != old_columns || !metadata_diff.empty())
    {
        LOG_WARNING(log, "Table structure in ZooKeeper is different from local table structure. Will ALTER.");

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(replica_path + "/metadata", zk_metadata_str, -1));
        ops.emplace_back(zkutil::makeSetRequest(replica_path + "/columns", zk_columns_str, -1));
        zookeeper->multi(ops);

        setTableStructure(std::move(zk_columns), metadata_diff);

        zookeeper->createOrUpdate(replica_path + "/metadata_version", std::to_string(metadata_stat.version), zkutil::CreateMode::Persistent);
        metadata_version = metadata_stat.version;
        LOG_DEBUG(log, "Set metadata version to: {} ", metadata_version);
    }
}

void StorageHaMergeTree::setTableStructure(
    ColumnsDescription new_columns, const HaMergeTreeTableMetadata::Diff & metadata_diff)
{
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    new_metadata.columns = new_columns;

    if (!metadata_diff.empty())
    {
        auto parse_key_expr = [] (const String & key_expr)
        {
            ParserNotEmptyExpressionList parser(false);
            auto new_sorting_key_expr_list = parseQuery(parser, key_expr, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

            ASTPtr order_by_ast;
            if (new_sorting_key_expr_list->children.size() == 1)
                order_by_ast = new_sorting_key_expr_list->children[0];
            else
            {
                auto tuple = makeASTFunction("tuple");
                tuple->arguments->children = new_sorting_key_expr_list->children;
                order_by_ast = tuple;
            }
            return order_by_ast;
        };

        if (metadata_diff.sorting_key_changed)
        {
            auto order_by_ast = parse_key_expr(metadata_diff.new_sorting_key);
            auto & sorting_key = new_metadata.sorting_key;
            auto & primary_key = new_metadata.primary_key;

            sorting_key.recalculateWithNewAST(order_by_ast, new_metadata.columns, global_context);

            if (primary_key.definition_ast == nullptr)
            {
                /// Primary and sorting key become independent after this ALTER so we have to
                /// save the old ORDER BY expression as the new primary key.
                auto old_sorting_key_ast = old_metadata.getSortingKey().definition_ast;
                primary_key = KeyDescription::getKeyFromAST(
                    old_sorting_key_ast, new_metadata.columns, global_context);
            }
        }

        if (metadata_diff.sampling_expression_changed)
        {
            auto sample_by_ast = parse_key_expr(metadata_diff.new_sampling_expression);
            new_metadata.sampling_key.recalculateWithNewAST(sample_by_ast, new_metadata.columns, global_context);
        }

        if (metadata_diff.skip_indices_changed)
            new_metadata.secondary_indices = IndicesDescription::parse(metadata_diff.new_skip_indices, new_columns, global_context);

        if (metadata_diff.constraints_changed)
            new_metadata.constraints = ConstraintsDescription::parse(metadata_diff.new_constraints);

        if (metadata_diff.ttl_table_changed)
        {
            if (!metadata_diff.new_ttl_table.empty())
            {
                ParserTTLExpressionList parser;
                auto ttl_for_table_ast = parseQuery(parser, metadata_diff.new_ttl_table, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                new_metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                    ttl_for_table_ast, new_metadata.columns, global_context, new_metadata.primary_key);
            }
            else /// TTL was removed
            {
                new_metadata.table_ttl = TTLTableDescription{};
            }
        }
    }

    /// Changes in columns may affect following metadata fields
    new_metadata.column_ttls_by_name.clear();
    for (const auto & [name, ast] : new_metadata.columns.getColumnTTLs())
    {
        auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, new_metadata.columns, global_context, new_metadata.primary_key);
        new_metadata.column_ttls_by_name[name] = new_ttl_entry;
    }

    if (new_metadata.partition_key.definition_ast != nullptr)
        new_metadata.partition_key.recalculateWithNewColumns(new_metadata.columns, global_context);

    if (!metadata_diff.sorting_key_changed) /// otherwise already updated
        new_metadata.sorting_key.recalculateWithNewColumns(new_metadata.columns, global_context);

    /// Primary key is special, it exists even if not defined
    if (new_metadata.primary_key.definition_ast != nullptr)
    {
        new_metadata.primary_key.recalculateWithNewColumns(new_metadata.columns, global_context);
    }
    else
    {
        new_metadata.primary_key = KeyDescription::getKeyFromAST(new_metadata.sorting_key.definition_ast, new_metadata.columns, global_context);
        new_metadata.primary_key.definition_ast = nullptr;
    }

    if (!metadata_diff.sampling_expression_changed && new_metadata.sampling_key.definition_ast != nullptr)
        new_metadata.sampling_key.recalculateWithNewColumns(new_metadata.columns, global_context);

    if (!metadata_diff.skip_indices_changed) /// otherwise already updated
    {
        for (auto & index : new_metadata.secondary_indices)
            index.recalculateWithNewColumns(new_metadata.columns, global_context);
    }

    if (!metadata_diff.ttl_table_changed && new_metadata.table_ttl.definition_ast != nullptr)
        new_metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
            new_metadata.table_ttl.definition_ast, new_metadata.columns, global_context, new_metadata.primary_key);

    /// Even if the primary/sorting/partition keys didn't change we must reinitialize it
    /// because primary/partition key column types might have changed.
    checkTTLExpressions(new_metadata, old_metadata);
    setProperties(new_metadata, old_metadata);

    auto table_id = getStorageID();
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(global_context, table_id, new_metadata);
}

void StorageHaMergeTree::cloneReplica(const String & cloned_replica, [[maybe_unused]]Coordination::Stat source_is_lost_stat, zkutil::ZooKeeperPtr & zookeeper)
{
    Stopwatch stopwatch;

    LOG_INFO(log, "Will mimic {}", cloned_replica);

    /// Before clone, force to set the table structure
    forceSetTableStructure(zookeeper);

    String source_path = zookeeper_path + "/replicas/" + cloned_replica;

    auto queue_task_lock = getActionLock(ActionLocks::ReplicationQueue);

    auto peer_updated_lsn = log_exchanger.getLSNStatus(cloned_replica).updated_lsn;
    auto self_committed_lsn = log_manager->getLSNStatus().committed_lsn;

    LogEntry::Vec prepared_entries;

    if (self_committed_lsn != 0 || peer_updated_lsn != 0)
    {
        log_manager->resetTo(peer_updated_lsn);
        queue.clear();
        prepared_entries = queue.pullLogs(zookeeper);
    }

    /// -----------------------------------------------------------------------
    /// Fetch all part
    String partition_id {}; /// empty partition id
    String filter {"1"}; /// 1 means always true

    /// Get the part list, i.e. source replica_path
    auto source_endpoint = zookeeper_path + "/replicas/" + cloned_replica;
    auto replica_address = getReplicaAddress(cloned_replica);

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(global_context.getSettingsRef());
    auto [user, password] = global_context.getInterserverCredentials();

    auto part_names = fetcher.fetchPartList(partition_id, filter,
            source_path, replica_address.host, replica_address.replication_port, timeouts,
            user, password, global_context.getInterserverScheme());


    // Generate GET log entries
    ActiveDataPartSet active_parts_set(format_version, part_names);

    // If local replica was lost and try to mimic others, we prefer to reuse those parts that
    // already exist in this replica.
    {
        clearOldTemporaryDirectories();

        auto localDataParts = getDataParts();
        MergeTreeData:: DataPartsVector parts_to_remove;
        for (auto & localPart : localDataParts)
        {
            auto cover_name = active_parts_set.getContainingPart(localPart->info);
            // part considered lost or changed because of other operation(e.g. merge),
            // ignore those op happened locally.
            if (cover_name != localPart->info.getPartName())
            {
                // TBD: remove local data might be dangerous, maybe need a knob to control if we
                // prefer to remove local part
                parts_to_remove.push_back(localPart);
            }
            else // duplicate, not need to fetch it from remote replica
            {
                active_parts_set.remove(localPart->info);
            }
        }

        if (!parts_to_remove.empty())
            removePartsFromWorkingSet(parts_to_remove, true);
        //clearOldPartsFromFilesystem();
    }

    auto active_parts = active_parts_set.getParts();

    size_t entries_size = prepared_entries.size() + active_parts.size();
    prepared_entries.reserve(entries_size);

    auto create_time = time(nullptr);
    for (auto & part_name : active_parts)
    {
        auto entry = std::make_shared<LogEntry>();
        entry->lsn = allocateLSN();
        entry->type = LogEntry::CLONE_PART;
        /// entry->storage_type = type;
        entry->source_replica = replica_name;
        entry->from_replica = cloned_replica;
        entry->new_parts.push_back(part_name);
        entry->create_time = create_time;
        prepared_entries.push_back(std::move(entry));
    }

    size_t marked_count = queue.markRedundantEntries(prepared_entries);
    if (marked_count)
        LOG_DEBUG(log, "Marked {} redundant entries while cloning replica.", marked_count);

    queue.write(std::move(prepared_entries));

    LOG_DEBUG(log, "Finished clone cost {} ms. Prepared {} parts to be fetched", stopwatch.elapsedMilliseconds(), active_parts.size());
}

void StorageHaMergeTree::getReplicaToClone(zkutil::ZooKeeperPtr & zookeeper, String & source_replica, Coordination::Stat & source_is_lost_stat)
{
    /// Only get delays of active replicas
    auto peer_delays = log_exchanger.getDelays();
    if (peer_delays.empty())
        throw Exception("No active replica to clone", ErrorCodes::ALL_REPLICAS_LOST);

    Int64 min_delay = std::numeric_limits<Int64>::max();
    /// TODO: String mimic_replica = global_context.getMimicReplica(this->database_name, this->table_name);
    String mimic_replica = "";

    for (auto && [replica, delay] : peer_delays)
    {
        /// Skip active but lost replica
        if (delay >= VERY_LARGE_DELAY)
            continue;

        /// config has assigned some replica to clone
        if (mimic_replica == replica)
        {
            source_replica = mimic_replica;
            break;
        }

        /// try to get a replica with min delay
        if (delay < min_delay)
        {
            min_delay = delay;
            source_replica = replica;
        }
    }

    if (source_replica.empty())
        throw Exception("All active replicas are lost", ErrorCodes::ALL_REPLICAS_LOST);

    if ("1" == zookeeper->get(zookeeper_path + "/replicas/" + source_replica + "/is_lost", &source_is_lost_stat))
        throw Exception("Selected a lost replica to clone. This is a bug", ErrorCodes::LOGICAL_ERROR);
}

void StorageHaMergeTree::cloneReplicaIfNeeded(zkutil::ZooKeeperPtr zookeeper)
{
    /// TODO:
    if (is_offline)
        return;

    String res = zookeeper->get(replica_path + "/is_lost");
    if (res == "0")
        return;

    String source_replica;

    Coordination::Stat source_is_lost_stat;

    getReplicaToClone(zookeeper, source_replica, source_is_lost_stat);
    /// Do not need to check whether source replica is active here.

    /// Will do repair from the selected replica.
    cloneReplica(source_replica, source_is_lost_stat, zookeeper);
    /// If repair fails to whatever reason, the exception is thrown, is_lost will remain "1" and the replica will be repaired later.

    /// If replica is repaired successfully, we remove is_lost flag.
    zookeeper->set(replica_path + "/is_lost", "0");
}

void StorageHaMergeTree::enterLeaderElection()
{
    auto callback = [this]()
    {
        LOG_INFO(log, "Became leader");

        is_leader = true;
        merge_selecting_task->activateAndSchedule();
    };

    try
    {
        leader_election = std::make_shared<zkutil::LeaderElection>(
            global_context.getSchedulePool(),
            zookeeper_path + "/leader_election",
            *current_zookeeper,    /// current_zookeeper lives for the lifetime of leader_election,
                                   ///  since before changing `current_zookeeper`, `leader_election` object is destroyed in `partialShutdown` method.
            callback,
            replica_name);
    }
    catch (...)
    {
        leader_election = nullptr;
        throw;
    }
}

void StorageHaMergeTree::exitLeaderElection()
{
    if (!leader_election)
        return;

    /// Shut down the leader election thread to avoid suddenly becoming the leader again after
    /// we have stopped the merge_selecting_thread, but before we have deleted the leader_election object.
    leader_election->shutdown();

    if (is_leader)
    {
        LOG_INFO(log, "Stopped being leader");

        is_leader = false;
        merge_selecting_task->deactivate();
    }

    /// Delete the node in ZK only after we have stopped the merge_selecting_thread - so that only one
    /// replica assigns merges at any given time.
    leader_election = nullptr;
}


void StorageHaMergeTree::mergeSelectingTask()
{
}

void StorageHaMergeTree::mutationsFinalizingTask()
{
}

    // Partition helpers
void StorageHaMergeTree::dropPartition(
    const ASTPtr & partition, bool detach, bool drop_part, const Context & query_context, bool throw_if_noop)
{
}

PartitionCommandsResultInfo StorageHaMergeTree::attachPartition(
    const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, const Context & query_context)
{
    return {};
}

void StorageHaMergeTree::replacePartitionFrom(
    const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & query_context)
{
}

void StorageHaMergeTree::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, const Context & query_context)
{
}

void StorageHaMergeTree::fetchPartition(
    const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, const String & from, const Context & query_context)
{
}

MutationCommands StorageHaMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & ) const
{
    return {};
    /// return queue.getFirstAlterMutationCommandsForPart(part);
}

void StorageHaMergeTree::startBackgroundMovesIfNeeded()
{
    if (areBackgroundMovesNeeded())
        background_moves_executor.start();
}

/// allocLSNAndSet() need two separated ZK operations which cannot be packed.
/// And it is ok to postpone the latter one, so that leave it as a request.
std::pair<UInt64, Coordination::RequestPtr> StorageHaMergeTree::allocLSNAndMakeSetRequest()
{
    auto node_prefix = getZKLatestLSNPath() + "/lsn-";
    auto created_path = getZooKeeper()->create(node_prefix, "", zkutil::CreateMode::EphemeralSequential);
    auto num = parse<UInt64>(created_path.substr(node_prefix.length()));
    LOG_TRACE(log, "Allocate new LSN-{}, and make a request", num);
    return {num, zkutil::makeSetRequest(getZKLatestLSNPath(), toString(num), -1)};
}

// Get unique block number from Zk
UInt64 StorageHaMergeTree::allocateBlockNumberDirect(zkutil::ZooKeeperPtr & zookeeper, const String &)
{
    // TBD: whether support deduplicate logic
    String block_numbers_path = zookeeper_path + "/block_numbers";
    String block_numbers_path_prefix = block_numbers_path + "/block-";

    auto block_path = zookeeper->create(block_numbers_path_prefix, "", zkutil::CreateMode::EphemeralSequential);

    auto block_number = parse<UInt64>(block_path.substr(block_numbers_path_prefix.length()));
    return block_number;
}

void StorageHaMergeTree::writeMutationLog(MutationLogElement::Type type, const MutationEntry & mutation_entry)
try
{
    /* TODO:
    auto log = global_context.getMutationLog();
    if (!log)
        return;

    MutationLogElement elem;
    elem.event_type = type;
    elem.event_time = time(nullptr);
    elem.database_name = database_name;
    elem.table_name = table_name;
    elem.mutation_id = mutation_entry.znode_name;
    elem.query_id = mutation_entry.query_id;
    elem.create_time = mutation_entry.create_time;
    elem.block_number = mutation_entry.block_number;
    for (const MutationCommand & command : mutation_entry.commands)
    {
        std::stringstream ss;
        formatAST(*command.ast, ss, false, true);
        elem.commands.push_back(ss.str());
    }

    log->add(elem);
    */
}
catch (...)
{
    tryLogCurrentException(log, __PRETTY_FUNCTION__);
}

UInt64 StorageHaMergeTree::allocLSNAndSet(const zkutil::ZooKeeperPtr & zookeeper)
{
    if (!zookeeper)
        throw Exception("No ZooKeeper session.", ErrorCodes::NO_ZOOKEEPER);

    auto node_prefix = getZKLatestLSNPath() + "/lsn-";
    auto created_path = zookeeper->create(node_prefix, "", zkutil::CreateMode::EphemeralSequential);
    auto lsn_str = created_path.substr(node_prefix.length());
    /// FIXME: we should check znode version here
    zookeeper->set(getZKLatestLSNPath(), lsn_str);
    LOG_TRACE(log, "Allocate and set new LSN-{}", lsn_str);
    return parse<UInt64>(lsn_str);
}

}

#pragma clang diagnostic push
