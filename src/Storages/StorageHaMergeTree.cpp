#include <Storages/StorageHaMergeTree.h>

#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/MutationLog.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/HaMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/HaMergeTreeReplicaEndpoint.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>

#include <regex>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-variable"

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
    extern const int PART_IS_LOST_FOREVER;
    extern const int BAD_TTL_EXPRESSION;
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
        current_zookeeper = getContext()->getZooKeeper();
    }
    else
    {
        current_zookeeper = getContext()->getAuxiliaryZooKeeper(zookeeper_name);
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

static NameOrderedSet collectPartitionsFromParts(const MergeTreeData::DataPartsVector & parts)
{
    NameOrderedSet partitions;
    for (auto & part : parts)
        partitions.insert(part->info.partition_id);
    return partitions;
}

StorageHaMergeTree::StorageHaMergeTree(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
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
    , merger_mutator(*this, getContext()->getSettingsRef().background_pool_size)
    /// , merge_strategy_picker(*this)
    , log_exchanger(*this)
    , queue(*this)
    , fetcher(*this)
    , background_executor(*this, getContext())
    , background_moves_executor(*this, getContext())
    , cleanup_thread(*this)
    // , part_check_thread(*this)
    , restarting_thread(*this)
    , alter_thread(*this)
    , allow_renaming(allow_renaming_)
    , replicated_fetches_pool_size(getContext()->getSettingsRef().background_fetches_pool_size)
{
    auto data_paths = getDataPaths();

    /// TODO: check data_paths
    log_manager = std::make_unique<HaMergeTreeLogManager>(
        data_paths.front() + "/log", getStorageID().getFullTableName() + " (HaLogManager)", !attach, *this);

    queue_updating_task = getContext()->getSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageHaMergeTree::queueUpdatingTask)", [this] { queueUpdatingTask(); });

    merge_selecting_task = getContext()->getMergeSelectSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageHaMergeTree::mergeSelectingTask)", [this] { mergeSelectingTask(); });

    /// Will be activated if we win leader election.
    merge_selecting_task->deactivate();

    mutations_finalizing_task = getContext()->getMutationSchedulePool().createTask(
        getStorageID().getFullTableName() + " (StorageHaMergeTree::mutationsFinalizingTask)", [this] { mutationsFinalizingTask(); });

    if (getContext()->hasZooKeeper() || getContext()->hasAuxiliaryZooKeeper(zookeeper_name))
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
                current_zookeeper = getContext()->getZooKeeper();
            }
            else
            {
                current_zookeeper = getContext()->getAuxiliaryZooKeeper(zookeeper_name);
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
    zookeeper->createIfNotExists(zookeeper_path + "/alter_partition_version", String());
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
    old_metadata.checkEquals(metadata_from_zk, metadata_snapshot->getColumns(), getContext());

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
            zookeeper = getContext()->getZooKeeper();
        else
            zookeeper = getContext()->getAuxiliaryZooKeeper(zookeeper_name);

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
    const ASTPtr &, const StorageMetadataPtr &, ContextPtr query_context, TableExclusiveLockHolder & table_lock)
{
    table_lock.release();   /// Truncate is done asynchronously.

    auto & settings = query_context->getSettingsRef();

    assertNotReadonly();
    if (!is_leader && !settings.ignore_leader_check)
        throw Exception("TRUNCATE cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    auto parts = getDataPartsVector();
    dropAllPartsInPartitions(zookeeper, collectPartitionsFromParts(parts), false, settings.replication_alter_partitions_sync > 0);
}

void StorageHaMergeTree::startup()
{
    if (is_readonly)
        return;

    try
    {
        queue.initialize(getDataParts());

        replica_endpoint_holder = std::make_unique<HaReplicaEndpointHolder>(
            replica_path, std::make_shared<HaMergeTreeReplicaEndpoint>(*this), getContext()->getHaReplicaHandler());

        InterserverIOEndpointPtr data_parts_exchange_ptr = std::make_shared<DataPartsExchange::Service>(*this, shared_from_this());
        [[maybe_unused]] auto prev_ptr = std::atomic_exchange(&data_parts_exchange_endpoint, data_parts_exchange_ptr);
        assert(prev_ptr == nullptr);
        getContext()->getInterserverIOHandler().addEndpoint(data_parts_exchange_ptr->getId(replica_path), data_parts_exchange_ptr);

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

    {
        auto lock = queue.lockQueue();
        /// Cancel logs pulling after background task were cancelled. It's still
        /// required because we can trigger pullLogsToQueue during manual OPTIMIZE,
        /// MUTATE, etc. query.
        queue.pull_log_blocker.cancelForever();
    }
    background_moves_executor.finish();

    auto data_parts_exchange_ptr = std::atomic_exchange(&data_parts_exchange_endpoint, InterserverIOEndpointPtr{});
    if (data_parts_exchange_ptr)
    {
        getContext()->getInterserverIOHandler().removeEndpointIfExists(data_parts_exchange_ptr->getId(replica_path));
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
    ContextPtr query_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    /** The `select_sequential_consistency` setting has two meanings:
    * 1. To throw an exception if on a replica there are not all parts which have been written down on quorum of remaining replicas.
    * 2. Do not read parts that have not yet been written to the quorum of the replicas.
    * For this you have to synchronously go to ZooKeeper.
    */
    if (query_context->getSettingsRef().select_sequential_consistency)
    {
        auto max_added_blocks = std::make_shared<HaMergeTreeQuorumAddedParts::PartitionIdToMaxBlock>(getMaxAddedBlocks());
        if (auto plan = reader.read(
                column_names, metadata_snapshot, query_info, query_context, max_block_size, num_streams, processed_stage, std::move(max_added_blocks)))
            query_plan = std::move(*plan);
        return;
    }

    if (auto plan = reader.read(column_names, metadata_snapshot, query_info, query_context, max_block_size, num_streams, processed_stage))
        query_plan = std::move(*plan);
}

Pipe StorageHaMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, query_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(query_context),
        BuildQueryPipelineSettings::fromContext(query_context));
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

std::optional<UInt64> StorageHaMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr query_context) const
{
    DataPartsVector parts;
    foreachCommittedParts([&](auto & part) { parts.push_back(part); }, query_context->getSettingsRef().select_sequential_consistency);
    return totalRowsByPartitionPredicateImpl(query_info, query_context, parts);
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
    auto host_port = getContext()->getInterserverIOAddress();
    auto table_id = getStorageID();

    HaMergeTreeAddress res;
    res.host = host_port.first;
    res.replication_port = host_port.second;
    res.queries_port = getContext()->getTCPPort();
    res.ha_port = getContext()->getHaTCPPort();
    res.database = table_id.database_name;
    res.table = table_id.table_name;
    res.scheme = getContext()->getInterserverScheme();
    return res;
}

HaMergeTreeAddress StorageHaMergeTree::getReplicaAddress(const String & replica_name_)
{
    String replica_host_path = zookeeper_path + "/replicas/" + replica_name_ + "/host";
    return HaMergeTreeAddress(getZooKeeper()->get(replica_host_path));
}

BlockOutputStreamPtr StorageHaMergeTree::write(const ASTPtr & /*query*/, [[maybe_unused]] const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    const auto storage_settings_ptr = getSettings();
    assertNotReadonly();

    const Settings & query_settings = query_context->getSettingsRef();
    [[maybe_unused]] bool deduplicate = storage_settings_ptr->replicated_deduplication_window != 0 && query_settings.insert_deduplicate;

    // TODO: should we also somehow pass list of columns to deduplicate on to the HaMergeTreeBlockOutputStream ?
    return std::make_shared<HaMergeTreeBlockOutputStream>(*this, metadata_snapshot, query_context);
}

bool StorageHaMergeTree::optimize(
    [[maybe_unused]]const ASTPtr &,
    [[maybe_unused]]const StorageMetadataPtr &,
    [[maybe_unused]]const ASTPtr & partition,
    [[maybe_unused]]bool final,
    [[maybe_unused]]bool deduplicate,
    [[maybe_unused]]const Names & deduplicate_by_columns,
    [[maybe_unused]]ContextPtr query_context)
{
    /// TODO:
    throw Exception("Not supported", ErrorCodes::SUPPORT_IS_DISABLED);
}

static String ast_to_str(ASTPtr query)
{
    if (!query)
        return "";
    return queryToString(query);
}

static void
compareAndUpdateMetadata(const StorageInMemoryMetadata & current_metadata, StorageInMemoryMetadata & future_metadata, HaMergeTreeTableMetadata & future_metadata_in_zk)
{

    if (ast_to_str(future_metadata.sorting_key.definition_ast) != ast_to_str(current_metadata.sorting_key.definition_ast))
    {
        /// We serialize definition_ast as list, because code which apply ALTER (setTableStructure) expect serialized non empty expression
        /// list here and we cannot change this representation for compatibility. Also we have preparsed AST `sorting_key.expression_list_ast`
        /// in KeyDescription, but it contain version column for VersionedCollapsingMergeTree, which shouldn't be defined as a part of key definition AST.
        /// So the best compatible way is just to convert definition_ast to list and serialize it. In all other places key.expression_list_ast should be used.
        future_metadata_in_zk.sorting_key = serializeAST(*extractKeyExpressionList(future_metadata.sorting_key.definition_ast));
    }

    if (ast_to_str(future_metadata.sampling_key.definition_ast) != ast_to_str(current_metadata.sampling_key.definition_ast))
        future_metadata_in_zk.sampling_expression = serializeAST(*extractKeyExpressionList(future_metadata.sampling_key.definition_ast));

    if (ast_to_str(future_metadata.partition_key.definition_ast) != ast_to_str(current_metadata.partition_key.definition_ast))
        future_metadata_in_zk.partition_key = serializeAST(*extractKeyExpressionList(future_metadata.partition_key.definition_ast));

    if (ast_to_str(future_metadata.table_ttl.definition_ast) != ast_to_str(current_metadata.table_ttl.definition_ast))
    {
        if (future_metadata.table_ttl.definition_ast)
            future_metadata_in_zk.ttl_table = serializeAST(*future_metadata.table_ttl.definition_ast);
        else /// TTL was removed
            future_metadata_in_zk.ttl_table = "";
    }

    String new_indices_str = future_metadata.secondary_indices.toString();
    if (new_indices_str != current_metadata.secondary_indices.toString())
        future_metadata_in_zk.skip_indices = new_indices_str;

    String new_projections_str = future_metadata.projections.toString();
    if (new_projections_str != current_metadata.projections.toString())
        future_metadata_in_zk.projections = new_projections_str;

    String new_constraints_str = future_metadata.constraints.toString();
    if (new_constraints_str != current_metadata.constraints.toString())
        future_metadata_in_zk.constraints = new_constraints_str;
}

void StorageHaMergeTree::alter(
    [[maybe_unused]] const AlterCommands & commands,
    [[maybe_unused]] ContextPtr query_context,
    [[maybe_unused]] TableLockHolder & table_lock_holder)
{
    assertNotReadonly();

    LOG_DEBUG(log, "Doing ALTER");

    auto table_id = getStorageID();

    if (commands.isSettingsAlter())
    {
        /// We don't replicate storage_settings_ptr ALTER. It's local operation.
        /// Also we don't upgrade alter lock to table structure lock.
        StorageInMemoryMetadata future_metadata = getInMemoryMetadata();
        commands.apply(future_metadata, query_context);

        /// merge_strategy_picker.refreshState();

        changeSettings(future_metadata.settings_changes, table_lock_holder);

        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(query_context, table_id, future_metadata);
        return;
    }

    auto & settings = query_context->getSettingsRef();
    UInt64 alter_skip_check = settings.alter_skip_check;

    auto zookeeper = getZooKeeper();

    auto current_metadata = getInMemoryMetadataPtr();
    auto mutation_commands
        = commands.getMutationCommands(*current_metadata, query_context->getSettingsRef().materialize_ttl_after_modify, query_context);

    queue.pullLogs(zookeeper);
    queue.checkAddMetadataAlter(mutation_commands);

    Coordination::Stat metadata_stat{};
    auto zk_metadata_str = zookeeper->get(zookeeper_path + "/metadata", &metadata_stat); /// XXX: is it unnecessary ?

    StorageInMemoryMetadata future_metadata = *current_metadata;
    commands.apply(future_metadata, query_context);

    HaMergeTreeTableMetadata future_metadata_in_zk(*this, current_metadata);
    compareAndUpdateMetadata(*current_metadata, future_metadata, future_metadata_in_zk);

    String new_metadata_str = future_metadata_in_zk.toString();
    String new_columns_str = future_metadata.columns.toString();

    /// Will try to set zk data

    auto zk_mutation_path = zookeeper_path + "/mutations";

    Coordination::Stat mutations_stat;
    zookeeper->get(zk_mutation_path, &mutations_stat);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/metadata", new_metadata_str, alter_skip_check < 2 ? metadata_version : -1));
    ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/columns", new_columns_str, -1));

    if (ast_to_str(current_metadata->settings_changes) != ast_to_str(future_metadata.settings_changes))
    {
        /// Just change settings
        StorageInMemoryMetadata metadata_copy = *current_metadata;
        metadata_copy.settings_changes = future_metadata.settings_changes;
        changeSettings(metadata_copy.settings_changes, table_lock_holder);
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(query_context, table_id, metadata_copy);
    }

    /// We can be sure, that in case of successful commit in zookeeper our
    /// version will increments by 1. Because we update with version check.
    int new_metadata_version = metadata_version + 1;
    if (alter_skip_check >= 2)
    {
        new_metadata_version = metadata_stat.version + 1;
    }

    auto create_time = time(nullptr);

    size_t alter_metadata_index = 0;
    std::optional<size_t> alter_data_index;

    /// Create mutation entry for altering metadata
    HaMergeTreeMutationEntry alter_entry;
    alter_entry.query_id = query_context->getInitialQueryId();
    alter_entry.create_time = create_time;
    alter_entry.source_replica = replica_name;
    alter_entry.alter_version = new_metadata_version;
    alter_entry.commands = mutation_commands;
    /// ADD_COLUMN don't need mutations
    mutation_commands.erase(
        std::remove_if(mutation_commands.begin(), mutation_commands.end(), [](auto & c) { return c.type == MutationCommand::ADD_COLUMN; }),
        mutation_commands.end());

    alter_entry.makeAlterMetadata();
    alter_entry.alter_info->have_mutation = !mutation_commands.empty();
    alter_entry.alter_info->columns_str = new_columns_str;
    alter_entry.alter_info->metadata_str = new_metadata_str;

    ops.emplace_back(zkutil::makeSetRequest(zk_mutation_path, String(), mutations_stat.version));
    alter_metadata_index = ops.size();
    ops.emplace_back(zkutil::makeCreateRequest(zk_mutation_path + "/", alter_entry.toString(), zkutil::CreateMode::PersistentSequential));

    if (!mutation_commands.empty())
    {
        /// Create mutation entry for mutation commands
        HaMergeTreeMutationEntry mutation_entry;
        mutation_entry.query_id = query_context->getInitialQueryId();
        mutation_entry.create_time = create_time;
        mutation_entry.source_replica = replica_name;
        mutation_entry.block_number = allocateBlockNumberDirect(zookeeper);
        mutation_entry.commands = mutation_commands;
        mutation_entry.alter_version = new_metadata_version;

        alter_data_index = ops.size();
        ops.emplace_back(
            zkutil::makeCreateRequest(zk_mutation_path + "/", mutation_entry.toString(), zkutil::CreateMode::PersistentSequential));
    }

    Coordination::Responses results;
    auto code = zookeeper->tryMulti(ops, results);

    if (code == Coordination::Error::ZBADVERSION)
    {
        /// if (results[0]->error != Coordination::Error::ZOK)
        ///     throw Exception(
        ///         "Metadata on replica is not up to date with common metadata in Zookeeper. Cannot alter", ErrorCodes::CANNOT_ASSIGN_ALTER);

        if (auto actual_columns_str = zookeeper->get(zookeeper_path + "/columns"); actual_columns_str != new_columns_str)
            throw Exception(
                "Another replica altered columns to " + actual_columns_str + ", but expected " + new_columns_str,
                ErrorCodes::CANNOT_ASSIGN_ALTER);

        if (auto actual_metadata_str = zookeeper->get(zookeeper_path + "/metadata"); actual_metadata_str != new_metadata_str)
            throw Exception(
                "Another replica altered metadata to " + actual_metadata_str + ", but expected " + new_metadata_str,
                ErrorCodes::CANNOT_ASSIGN_ALTER);

        LOG_INFO(log, "Another replica altered columns/metadata to expeceted values. Skip this query.");
        return; /// Let queue schedule the mutation logs from other replica
    }

    /// Must throw excpetion here if failed
    zkutil::KeeperMultiException::check(code, ops, results);
    LOG_INFO(log, "Updated shared metadata nodes in ZooKeeper.");

    table_lock_holder.reset();

    auto wait_timeout = settings.mutations_wait_timeout;
    UInt64 alter_data_sync = settings.ha_alter_data_sync;
    /// Waiting alter data means that must wait alter metadata, so override later
    UInt64 alter_metadata_sync = std::max(UInt64(settings.ha_alter_metadata_sync), alter_data_sync);

    /// Anyway, wait data mutations first if needed
    if (alter_data_sync && alter_data_index)
    {
        LOG_DEBUG(log, "Waiting for replicas to apply metadata & data changes. Sync level {}", alter_data_sync);
        String mutation_path = dynamic_cast<const Coordination::CreateResponse &>(*results[*alter_data_index]).path_created;
        String mutation_node = mutation_path.substr(mutation_path.find_last_of('/') + 1);
        waitMutation(mutation_node, false, alter_data_sync, wait_timeout);
        LOG_DEBUG(log, "Data & metadata changes applied");
    }
    /// Still need to wait metadata
    UInt64 actual_data_sync = alter_data_index ? alter_data_sync : 0;
    if (alter_metadata_sync > actual_data_sync)
    {
        LOG_DEBUG(log, "Waiting for replicas to apply metadata changes. Sync level {}",  alter_metadata_sync);
        String mutation_path = dynamic_cast<const Coordination::CreateResponse &>(*results[alter_metadata_index]).path_created;
        String mutation_node = mutation_path.substr(mutation_path.find_last_of('/') + 1);
        waitMutation(mutation_node, true, alter_metadata_sync, wait_timeout);
        LOG_DEBUG(log, "Metadata changes applied");
    }
}

void StorageHaMergeTree::mutate(const MutationCommands & commands, ContextPtr query_context)
{
    HaMergeTreeMutationEntry mutation_entry;
    mutation_entry.query_id = query_context->getInitialQueryId();
    mutation_entry.source_replica = replica_name;
    mutation_entry.commands = commands;

    if (String query_id = query_context->getSettingsRef().mutation_query_id; !query_id.empty())
        mutation_entry.query_id = query_id;

    if (!mutation_entry.extractPartitionIds(*this, query_context))
        return;

    const String mutations_path = zookeeper_path + "/mutations";
    auto zookeeper = getZooKeeper();

    String max_znode_checked_for_dup;
    /// Update the mutations_path node when creating the mutation and check its version to ensure that
    /// nodes for mutations are created in the same order as the corresponding block numbers.
    /// Should work well if the number of concurrent mutation requests is small.
    while (true)
    {
        Coordination::Stat mutations_stat;
        zookeeper->get(mutations_path, &mutations_stat);

        /// deduplicate requests based on query id
        if (auto dup = queue.findDuplicateMutationByQueryId(zookeeper, mutation_entry, max_znode_checked_for_dup); dup)
        {
            mutation_entry.znode_name = dup->znode_name;
            break;
        }

        /// all committed parts whose min_block < mutation's block number are candidates for this mutation
        mutation_entry.block_number = allocateBlockNumberDirect(zookeeper);
        mutation_entry.create_time = time(nullptr);

        /// The following version check guarantees the linearizability property for any pair of mutations:
        /// mutation with higher sequence number is guaranteed to have higher block numbers in every partition
        /// (and thus will be applied strictly according to sequence numbers of mutations)
        Coordination::Requests requests;
        /// Use SetRequest instead of CheckRequest here because CAS checks znode's data version
        /// which is only changed by "set"
        requests.emplace_back(zkutil::makeSetRequest(mutations_path, String(), mutations_stat.version));
        requests.emplace_back(zkutil::makeCreateRequest(
            mutations_path + "/", mutation_entry.toString(), zkutil::CreateMode::PersistentSequential));

        Coordination::Responses responses;
        auto rc = zookeeper->tryMulti(requests, responses);
        if (rc == Coordination::Error::ZOK)
        {
            const String & path_created =
                dynamic_cast<const Coordination::CreateResponse *>(responses[1].get())->path_created;
            mutation_entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);
            LOG_INFO(log, "Created mutation with ID {}" , mutation_entry.znode_name);
            writeMutationLog(MutationLogElement::MUTATION_START, mutation_entry);
            break;
        }
        else if (rc == Coordination::Error::ZBADVERSION)
        {
            LOG_WARNING(log, "Version conflict when trying to create a mutation node, retrying...");
            continue;
        }
        else
            throw Coordination::Exception("Unable to create a mutation znode", rc);
    }

    waitMutation(
        mutation_entry.znode_name,
        /*is_alter_metadata=*/false,
        query_context->getSettingsRef().mutations_sync,
        query_context->getSettingsRef().mutations_wait_timeout);
}

void StorageHaMergeTree::waitMutation(
    const String & znode_name, bool is_alter_metadata, size_t mutations_sync, UInt64 timeout_seconds) const
{
    if (mutations_sync == 0)
        return;

    /// we have to wait
    auto zookeeper = getZooKeeper();
    Strings replicas;
    if (mutations_sync == 2) /// wait for all non-lost replicas
    {
        Strings all_replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
        for (auto & replica : all_replicas)
        {
            if (isReplicaLostOrOffline(zookeeper, replica))
                continue;

            replicas.push_back(replica);
        }
    }
    else if (mutations_sync == 1) /// just wait for ourself
    {
        replicas.push_back(replica_name);
    }

    waitMutationToFinishOnReplicas(replicas, znode_name, is_alter_metadata, timeout_seconds);
}

void StorageHaMergeTree::waitMutationToFinishOnReplicas(
    const Strings & replicas, const String & mutation_id, bool is_alter_metadata, UInt64 timeout_seconds) const
{
    if (replicas.empty())
        return;

    Stopwatch timer;
    NameSet inactive_replicas;
    for (auto & replica : replicas)
    {
        LOG_DEBUG(log, "Waiting for {} to apply mutation {}", replica, mutation_id);
        zkutil::EventPtr wait_event = std::make_shared<Poco::Event>();

        while (!partial_shutdown_called)
        {
            auto zookeeper = getZooKeeper();
            /// Mutation maybe killed or whole replica was deleted.
            /// Wait event will unblock at this moment.
            Coordination::Stat exists_stat;
            if (!zookeeper->exists(zookeeper_path + "/mutations/" + mutation_id, &exists_stat, wait_event))
            {
                throw Exception("Mutation " + mutation_id + " was killed, manually removed or table was dropped", ErrorCodes::UNFINISHED);
            }

            /// Replica could be inactive.
            if (!zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
            {
                LOG_WARNING(log, "Replica {} is not active during mutation. Mutation will be done asynchronously when replica becomes active.", replica);
                inactive_replicas.emplace(replica);
                break;
            }

            String mutation_pointer = zookeeper_path + "/replicas/" + replica + "/mutation_pointer";
            std::string mutation_pointer_value;
            /// Replica could be removed
            if (!zookeeper->tryGet(mutation_pointer, mutation_pointer_value, nullptr, wait_event))
            {
                LOG_WARNING(log, "Replica {} was removed", replica);
                break;
            }
            else if (mutation_pointer_value >= mutation_id) /// Maybe we already processed more fresh mutation
                break;                                      /// (numbers like 0000000000 and 0000000001)

            if (timeout_seconds && timer.elapsedSeconds() > timeout_seconds)
                throw Exception("Timeout when waiting for replica " + replica + " to finish mutation " + mutation_id +
                                ", operation will be done asynchronously", ErrorCodes::UNFINISHED);

            /// Replica can become inactive, so wait with timeout and recheck it
            if (wait_event->tryWait(1000))
                continue;

            auto status = queue.getPartialMutationsStatus(mutation_id, is_alter_metadata);
            if (status && !status->is_done && !status->latest_fail_reason.empty())
                throw Exception("Exception happened during executing of mutation " + mutation_id +
                                " with part " + status->latest_failed_part + " reason: " + status->latest_fail_reason,
                                ErrorCodes::UNFINISHED);
        }

        if (partial_shutdown_called)
            throw Exception("Mutation is not finished because table shutdown was called. It will be done after table restart.",
                            ErrorCodes::UNFINISHED);
    }

    if (!inactive_replicas.empty())
    {
        throw Exception(toString(inactive_replicas.size()) + " replicas are inactive right now, mutation will be done asynchronously",
                        ErrorCodes::UNFINISHED);
    }
}

void StorageHaMergeTree::waitAllMutations(UInt64 max_wait_milliseconds)
{
    Stopwatch watch;
    /// Let's fetch new log entries firstly
    log_exchanger.resetReplicaStatusAndConnection(); /// make sure replica status is up-to-date
    queue.pullLogsToQueue(getZooKeeper(), {}, /*ignore_backoff=*/true);

    while (queue.countUnfinishedMutations(/*include_alter_metadata=*/true))
    {
        if (partial_shutdown_called)
            throw Exception("Shutdown is called for table", ErrorCodes::ABORTED);

        if (max_wait_milliseconds && watch.elapsedMilliseconds() > max_wait_milliseconds)
            throw Exception("Timeout when wait for mutations to finish", ErrorCodes::UNFINISHED);

        background_executor.triggerTask();

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

std::vector<MergeTreeMutationStatus> StorageHaMergeTree::getMutationsStatus() const
{
    return queue.getMutationsStatus();
}

CancellationCode StorageHaMergeTree::killMutation(const String & mutation_id)
{
    assertNotReadonly();

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();

    LOG_TRACE(log, "Killing mutation {}", mutation_id);

    auto mutation_entry = queue.removeMutation(zookeeper, mutation_id);
    if (!mutation_entry)
        return CancellationCode::NotFound;

    /// After this point no new part mutations will start and part mutations that still exist
    /// in the queue will be skipped.

    /// Cancel already running part mutations.
    getContext()->getMergeList().cancelHaPartMutations(getStorageID(), *mutation_entry);
    return CancellationCode::CancelSent;
}


void StorageHaMergeTree::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    getContext()->checkTableCanBeDropped(table_id.database_name, table_id.table_name, getTotalActiveSizeInBytes());
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
            zookeeper->set(replica_path + "/host", getHaMergeTreeAddress().toString());
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

    if (action_type == ActionLocks::ReplicationQueue)
        return queue.actions_blocker.cancel();

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

CheckResults StorageHaMergeTree::checkData([[maybe_unused]]const ASTPtr & query, [[maybe_unused]]ContextPtr query_context)
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


bool StorageHaMergeTree::scheduleDataProcessingJob(IBackgroundJobExecutor & executor)
{
    /// If replication queue is stopped exit immediately as we successfully executed the task
    if (queue.actions_blocker.isCancelled())
        return false;

    /// This object will mark the element of the queue as running.
    size_t num_avail_fetches = 0;
    auto selected_entry = queue.selectEntryToProcess(num_avail_fetches);

    if (!selected_entry->valid())
        return false;

    if (selected_entry->getExecuting()->type == LogEntry::GET_PART)
    {
        executor.execute({[this, selected_entry]() mutable { return processQueueEntry(selected_entry); }, PoolType::FETCH});
        return true;
    }
    else
    {
        executor.execute({[this, selected_entry]() mutable { return processQueueEntry(selected_entry); }, PoolType::MERGE_MUTATE});
        return true;
    }
}

bool StorageHaMergeTree::processQueueEntry(HaQueueExecutingEntrySetPtr executing_set)
{
    bool success = false;
    bool remove_bad_entry = false;

    SCOPE_EXIT(
        try {
            if (success)
                queue.removeProcessedEntries(executing_set->collect());
            else if (remove_bad_entry && executing_set)
                queue.removeProcessedEntry(executing_set->getExecuting());
        } catch (...) {
            // make clang-format happy
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        });

    auto now = time(nullptr);
    auto settings = getSettings();
    auto entry = executing_set->getExecuting();

    try
    {
        LOG_DEBUG(log, "Executing entry {} ", entry->toDebugString());

        success = executeLogEntry(executing_set);

        /// executing entry has been moved to other
        if (!executing_set->valid())
        {
            LOG_TRACE(log, "entry {} became invalid, result: {}", entry->toDebugString(), success);
            return true;
        }

        if (!success && entry->first_attempt_time + Int64(settings->ha_mark_lost_replica_timeout) < now
            && entry->num_tries > settings->ha_max_log_try_times)
        {
            remove_bad_entry = true;
            LOG_ERROR(log, "Marked log {} done of which execution may not be successful anymore.", entry->toDebugString());
        }

        LOG_TRACE(log, "{} to execute entry {} ", (success ? "Succeed" : "Failed"), entry->toDebugString());
    }
    catch (const Exception & e)
    {
        queue.onLogExecutionError(entry, std::current_exception());

        if (e.code() == ErrorCodes::PART_IS_LOST_FOREVER && (entry->type == LogEntry::GET_PART || entry->type == LogEntry::CLONE_PART)
            && entry->first_attempt_time + time_t(settings->ha_update_replica_stats_period * 3) < now && entry->num_tries > 3)
        {
            remove_bad_entry = true;
            LOG_ERROR(log, "Marked log {} done because {}", entry->toDebugString(), e.displayText());
        }
        if (e.code() == ErrorCodes::PART_IS_LOST_FOREVER && (entry->type == LogEntry::MERGE_PARTS || entry->type == LogEntry::MUTATE_PART)
            && entry->num_tries > settings->ha_max_log_try_times)
        {
            remove_bad_entry = true;
            LOG_ERROR(log, "Marked log {} done because {}", entry->toDebugString(), e.displayText());
        }
        if (e.code() == ErrorCodes::NO_REPLICA_HAS_PART)
        {
            /// If no one has the right part, probably not all replicas work; We will not write to log with Error level.
            LOG_INFO(log, "{} {} {}", __func__, entry->toDebugString(), e.displayText());
        }
        else if (e.code() == ErrorCodes::ABORTED)
        {
            /// Interrupted merge or downloading a part is not an error.
            LOG_INFO(log, "{} {} {}", __func__, entry->toDebugString(), e.message());
        }
        else if (e.code() == ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
        {
            /// Part cannot be added temporarily
            LOG_INFO(log, "{} {} {}", __func__, entry->toDebugString(), e.displayText());
        }
        else
        {
            LOG_ERROR(log, "Failed to execute {} : {}", entry->toDebugString(), getCurrentExceptionMessage(true));
        }
    }
    catch (...)
    {
        queue.onLogExecutionError(entry, std::current_exception());

        LOG_ERROR(log, "Failed to execute {} : {}", entry->toDebugString(), getCurrentExceptionMessage(true));
    }

    if (success || remove_bad_entry)
        return true;

    return false;
}

bool StorageHaMergeTree::executeLogEntry(HaQueueExecutingEntrySetPtr & executing_set)
{
    auto & entry = executing_set->getExecuting();

    switch (entry->type)
    {
        case LogEntry::GET_PART:
            return executeFetch(executing_set);

        case LogEntry::CLONE_PART:
            return (entry->source_replica == replica_name) ? executeFetch(executing_set) : true;

        case LogEntry::MERGE_PARTS:
            return executeMerge(executing_set);

        case LogEntry::MUTATE_PART:
            return executeMutate(executing_set);

        case LogEntry::DROP_RANGE:
            return executeDropRange(executing_set);

        case LogEntry::REPLACE_RANGE:
            return executeReplaceRange(executing_set);

        case LogEntry::BAD_LOG:
            return true;

        default:
            throw Exception("Log Type is not support", ErrorCodes::LOGICAL_ERROR);
    }
}

/// TODO: update it according to community
bool StorageHaMergeTree::executeFetch(HaQueueExecutingEntrySetPtr & executing_set)
{
    auto & entry = executing_set->getExecuting();
    auto new_part_name = entry->new_part_name;

    /// Some part has already covered this one, DO NOT fetch it.
    if (auto covered_part = getActiveContainingPart(new_part_name))
    {
        LOG_TRACE(log, "Cancel fetching part {} covered by {}", new_part_name, covered_part->name);

        /// If part already covered by local partition update quorum success
        /// if (entry->quorum)
        /// {
        ///     updateQuorum(new_part_name);
        /// }
        return true;
    }

    /// TODO: fix later
    if (entry->type == LogEntry::GET_PART && entry->source_replica == replica_name)
    {
        LOG_WARNING(log, "Try to fetch a part where source replica of GET_PART is self: {} ", entry->toDebugString());
        return true;
    }

    return fetchPartHeuristically(
        executing_set,
        new_part_name,
        (LogEntry::CLONE_PART == entry->type) ? entry->from_replica : entry->source_replica,
        false,
        entry->quorum);
}

/// TODO: refactor this function to findReplicaHavingCoveringPart
bool StorageHaMergeTree::fetchPartHeuristically(
    const HaQueueExecutingEntrySetPtr & executing_set,
    const String & part_name,
    const String & backup_replica,
    bool to_detached,
    size_t quorum,
    bool incrementally)
{
    bool all_success = false;
    auto candidate_replicas = log_exchanger.findActiveContainingPart(part_name, all_success);

    if (candidate_replicas.empty())
    {
        if (log_exchanger.isLostReplica(backup_replica))
            throw Exception(
                "Part " + part_name + " is lost forever because no active replica has it and source replica is lost",
                ErrorCodes::PART_IS_LOST_FOREVER);
        else if (all_success && executing_set && executing_set->getExecuting()->create_time + 1800 < time(nullptr))
            throw Exception(
                "Part " + part_name + " is lost forever because no active or failed replica has it", ErrorCodes::PART_IS_LOST_FOREVER);
        else
            throw Exception("No active replica has part " + part_name, ErrorCodes::NO_REPLICA_HAS_PART);
    }

    /// Remove this
    if (log->trace())
    {
        std::ostringstream oss;
        oss << "Candidate replica for part " << part_name << ": ";
        for (auto & p : candidate_replicas)
            oss << p.replica << " " << p.payload << " " << p.containing_part << "; ";
        LOG_TRACE(log, oss.str());
    }

    /// the "payload" field measures the load of the replica
    std::sort(candidate_replicas.begin(), candidate_replicas.end(), [](auto & lhs, auto & rhs) { return lhs.payload < rhs.payload; });
    String replica_to_fetch = candidate_replicas[0].replica;
    String part_to_fetch = part_name;

    for (auto & candidate : candidate_replicas)
    {
        if (candidate.containing_part.empty())
            continue;
        /// TODO: checkPartNameAddable
        replica_to_fetch = candidate.replica;
        part_to_fetch = candidate.containing_part;
        break;
    }

    if (MergeTreePartInfo::fromPartName(part_to_fetch, format_version).isFakeDropRangePart())
    {
        LOG_DEBUG(log, "Part {} is already dropped by replica {} inside {}", part_name, replica_to_fetch, part_to_fetch);
        return true;
    }

    return fetchPart(
        executing_set,
        part_to_fetch,
        zookeeper_path + "/replicas/" + replica_to_fetch,
        to_detached,
        quorum,
        false, // to_repair
        incrementally);
}

bool StorageHaMergeTree::fetchPart(
    const HaQueueExecutingEntrySetPtr & executing_set,
    const String & part_name,
    const String & source_replica_path,
    bool to_detached,
    size_t quorum,
    bool,
    bool incrementally)
{
    std::unique_ptr<FetchingPartToExecutingEntrySet::Handle> handle;
    if (executing_set)
    {
        handle = current_fetching_parts_with_entries.insertOrMerge(part_name, executing_set);
        if (!handle)
            return false; /// assume fail to execute
    }
    else
    {
        /// TODO: optimize for this case
    }

    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Fetching part {} from {}", part_name, source_replica_path);

    TableLockHolder table_lock_holder;
    if (!to_detached)
        table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

    /// Logging
    Stopwatch stopwatch;
    MutableDataPartPtr part;
    DataPartsVector replaced_parts;

    auto write_part_log = [&](const ExecutionStatus & execution_status) {
        writePartLog(PartLogElement::DOWNLOAD_PART, execution_status, stopwatch.elapsed(), part_name, part, replaced_parts, nullptr);
    };

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(getContext());
    auto credentials = getContext()->getInterserverCredentials();
    String interserver_scheme = getContext()->getInterserverScheme();

    try
    {
        HaMergeTreeAddress address(zookeeper->get(source_replica_path + "/host"));
        if (interserver_scheme != address.scheme)
            throw Exception(
                "Interserver schemes are different '" + interserver_scheme + "' != '" + address.scheme + "', can't fetch part from "
                    + address.host,
                ErrorCodes::LOGICAL_ERROR);

        part = fetcher.fetchPart(
            getInMemoryMetadataPtr(),
            getContext(),
            part_name,
            source_replica_path,
            address.host,
            address.replication_port,
            timeouts,
            credentials->getUser(),
            credentials->getPassword(),
            interserver_scheme,
            replicated_fetches_throttler,
            to_detached,
            "");

        if (!to_detached)
        {
            MergeTreeData::Transaction transaction(*this);
            renameTempPartAndReplace(part, nullptr, &transaction);

            /// TODO check check sums
            /// if (!quorum || updateQuorum(part_name))
            {
                transaction.commit();
                if (handle && part->getState() == DataPartState::Committed)
                    handle->getExecutingLog()->actual_committed_part = part->name;
            }
            /// else
            /// {
            ///     transaction.rollback();
            ///     fetch_success = false;
            ///     LOG_WARNING(
            ///         log,
            ///         "Update quorum status of local " << part_name << " failed. "
            ///                                          << "Not fetch this partition.");
            /// }

            write_part_log({});
        }
        else
        {
            // The fetched part is valuable and should not be cleaned like a temp part.
            part->is_temp = false;
            part->renameTo("detached/" + part_name, true);
        }
    }
    catch (const Exception & e)
    {
        /// The same part is being written right now (but probably it's not committed yet).
        /// We will check the need for fetch later.
        if (e.code() == ErrorCodes::DIRECTORY_ALREADY_EXISTS)
            return false;

        throw;
    }
    catch (...)
    {
        if (!to_detached)
            write_part_log(ExecutionStatus::fromCurrentException());

        throw;
    }

    LOG_DEBUG(log, "Fetched part {} from {} {}", part_name, source_replica_path, (to_detached ? " (to 'detach' directory)" : ""));
    return true;
}

bool StorageHaMergeTree::executeMerge(HaQueueExecutingEntrySetPtr & executing_set)
{
    auto & entry = *executing_set->getExecuting();

    auto new_part_name = entry.new_part_name;

    const auto storage_settings_ptr = getSettings();

    if (getActiveContainingPart(new_part_name))
    {
        LOG_DEBUG(log, "Cancel merging part {} by covered part(s)", new_part_name);
        merge_selecting_task->schedule();
        return true;
    }


    /// TODO: check addable

    MergeTreeData::DataPartsVector parts;

    // handle the case that source part not exist and cannot merge
    bool have_all_parts = true;
    for (const String & name : entry.source_parts)
    {
        MergeTreeData::DataPartPtr part = getActiveContainingPart(name);
        if (!part)
        {
            have_all_parts = false;
            break;
        }

        if (part->name != name)
        {
            LOG_WARNING(
                log, "Part {} is covered by {} but should be merged into {}. This shoudn't happen often.", name, part->name, new_part_name);
            have_all_parts = false;
            break;
        }

        parts.push_back(part);
    }

    if (!have_all_parts)
    {
        if (entry.source_replica == replica_name)
        {
            LOG_ERROR(
                log,
                "Lgical error: generate merge log by self but don't have all parts for merge {} Will mark {} executed",
                new_part_name,
                entry.toDebugString());
            return true;
        }

        LOG_DEBUG(log, "Don't have all parts for merge {}; will try to fetch it instead");

        try
        {
            fetchPartHeuristically(executing_set, new_part_name, entry.source_replica, false);
            return true;
        }
        catch (Exception & e)
        {
            e.addMessage("(while fetching part because of don't have all parts for merge " + new_part_name + ")");
            throw e;
        }
        catch (...)
        {
            throw;
        }
    }

    {
        std::lock_guard current_merging_parts_lock(current_merging_parts_mutex);
        if (current_merging_parts.count(entry.new_part_name))
            throw Exception("Duplicate merging part " + entry.new_part_name, ErrorCodes::LOGICAL_ERROR);
        current_merging_parts.emplace(entry.new_part_name);
    }

    SCOPE_EXIT({
        std::lock_guard current_merging_parts_lock(current_merging_parts_mutex);
        current_merging_parts.erase(entry.new_part_name);
    });

    // start to make the main merge work
    size_t estimated_space_for_merge = MergeTreeDataMergerMutator::estimateNeededDiskSpace(parts);

    auto reserved_space = reserveSpace(estimated_space_for_merge);

    FutureMergedMutatedPart future_merged_part(parts);
    if (future_merged_part.name != new_part_name)
    {
        throw Exception(
            "Future merged part name `" + future_merged_part.name + "` differs from part name in log entry: `"
                + new_part_name + "`",
            ErrorCodes::BAD_DATA_PART_NAME);
    }

    auto table_id = getStorageID();
    MergeList::EntryPtr merge_entry = getContext()->getMergeList().insert(getStorageID(), future_merged_part);

    // Without fetch firstly strategy deployed in product, we find out merge in replica(1:N) settings are
    // quite costly, and too many OOMs. we try fetch if possible
    if (!storage_settings_ptr->prefer_merge_than_fetch && replica_name != entry.source_replica)
    {
        try
        {
            fetchPartHeuristically(executing_set, new_part_name, entry.source_replica, false);
            return true;
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::NO_REPLICA_HAS_PART)
                LOG_DEBUG(log, "{}, {} ", __func__, e.displayText());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    MergeTreeData::MutableDataPartPtr part;
    Stopwatch stopwatch;

    auto write_part_log = [&] (const ExecutionStatus & status)
    {
        writePartLog(PartLogElement::MERGE_PARTS, status, stopwatch.elapsed(), entry.new_part_name, part, parts, merge_entry.get());
    };

    try
    {
        auto table_lock = lockForShare(RWLockImpl::NO_QUERY, storage_settings_ptr->lock_acquire_timeout_for_background_operations);

        auto metadata_snapshot = getInMemoryMetadataPtr();

        part = merger_mutator.mergePartsToTemporaryPart(
            future_merged_part,
            metadata_snapshot,
            *merge_entry,
            table_lock,
            entry.create_time,
            getContext(),
            reserved_space,
            false, // entry.duduplicate
            {}, /// entry.duduplicate_by_columns
            merging_params);

        MergeTreeData::Transaction transaction(*this);
        merger_mutator.renameMergedTemporaryPart(part, parts, &transaction);
        /// checkPartChecksumsAndCommit(transaction, part);
        transaction.commit();
        if (part->getState() == DataPartState::Committed)
            entry.actual_committed_part = part->name;

        merge_selecting_task->schedule();

        write_part_log({});
        return true;
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }
}

bool StorageHaMergeTree::executeMutate(HaQueueExecutingEntrySetPtr & executing_set)
{
    auto & entry = *executing_set->getExecuting();

    auto & src_part_name = entry.source_parts.front();
    auto & new_part_name = entry.new_part_name;
    auto new_part_info = MergeTreePartInfo::fromPartName(new_part_name, format_version);

    auto storage_settings_ptr = getSettings();

    /// if new part is covered by committed parts, skip the log
    if (auto containing_part = getActiveContainingPart(new_part_name))
    {
        LOG_DEBUG(log, "Cancel mutating part to {}: covered by part {}", new_part_name, containing_part->name);
        merge_selecting_task->schedule();
        return true;
    }

    /// TODO: check addable ?

    /// get the corresponding mutation commands, skip the log if not found (mutation is killed)
    MutationCommands commands = queue.getMutationCommands(new_part_info.mutation);
    if (commands.empty())
    {
        LOG_WARNING(
            log,
            "Cancel mutating {} to {}: can't find mutation with version {}, maybe it was killed",
            src_part_name,
            new_part_name,
            new_part_info.mutation);
        merge_selecting_task->schedule();
        return true;
    }

    bool have_src_part = false;
    auto source_part = getActiveContainingPart(src_part_name);
    if (source_part && source_part->name == src_part_name)
        have_src_part = true;
    else if (source_part)
        LOG_WARNING(log, "Trying to mutate {} but it's covered by {}. Fetch mutated part instead", src_part_name, source_part->name);

    /// if source part is not present, try to fetch mutated part from other replicas
    if (!have_src_part)
    {
        if (entry.source_replica == replica_name)
        {
            LOG_ERROR(log, "Current replica generates a log to mutate {} but source part is not found, skip the log", src_part_name);
            return true;
        }
        LOG_DEBUG(log, "Don't have {} to mutate, will try to fetch mutated part instead", src_part_name);
        try
        {
            fetchPartHeuristically(
                executing_set,
                new_part_name,
                entry.source_replica,
                false, /// detached
                0, /// quorum
                0);
                /// TODO: getContext()->getSettingsRef().enable_fetch_part_incrementally);
            return true;
        }
        catch (Exception & e)
        {
            e.addMessage("(while fetching part because of don't have part " + src_part_name + " to mutate)");
            throw e;
        }
        catch (...)
        {
            throw;
        }
    }

    /// TODO shall we maintain current_mutating_parts?

    size_t estimated_space_for_result = MergeTreeDataMergerMutator::estimateNeededDiskSpace({source_part});

    /// Once we mutate part, we must reserve space on the same disk, because mutations can possibly create hardlinks.
    /// Can throw an exception.
    ReservationPtr reserved_space = reserveSpace(estimated_space_for_result, source_part->volume);

    /// if fetch failed, mutate part locally
    FutureMergedMutatedPart future_part;
    future_part.name = new_part_name;
    /// TODO: future_part.uuid = entry.new_part_uuid;
    future_part.parts.push_back(source_part);
    future_part.part_info = new_part_info;
    future_part.updatePath(*this, reserved_space);
    future_part.type = source_part->getType();

    MergeList::EntryPtr merge_entry = getContext()->getMergeList().insert(getStorageID(), future_part);

    MergeTreeData::MutableDataPartPtr part;
    Stopwatch stopwatch;

    auto write_part_log = [&] (const ExecutionStatus & status)
    {
        writePartLog(
            PartLogElement::MUTATE_PART, status, stopwatch.elapsed(),
            new_part_name, part, future_part.parts, merge_entry.get());
    };

    try
    {
        auto table_lock = lockForShare(
                RWLockImpl::NO_QUERY, storage_settings_ptr->lock_acquire_timeout_for_background_operations);
        StorageMetadataPtr metadata_snapshot = getInMemoryMetadataPtr();

        part = merger_mutator.mutatePartToTemporaryPart(
            future_part, metadata_snapshot, commands, *merge_entry, entry.create_time, getContext(), reserved_space, table_lock);

        renameTempPartAndReplace(part);

        if (part->getState() == DataPartState::Committed)
            entry.actual_committed_part = part->name;

        merge_selecting_task->schedule();

        /// dropQueryCache();
        write_part_log({});
        return true;
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }
}

MergeTreeMutableDataPartPtr StorageHaMergeTree::createDropRangePart(
    const String & drop_range_name, const MergeTreePartInfo & drop_range_info, const StorageMetadataPtr & metadata_snapshot)
{
    auto storage_columns = metadata_snapshot->getColumns().getAllPhysical();

    auto disk = getStoragePolicy()->getAnyDisk();
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + drop_range_name, disk, 0);

    /// Create fake drop range part
    auto drop_range_part
        = createPart(drop_range_name, MergeTreeDataPartType::COMPACT, drop_range_info, single_disk_volume, "tmp_delete_" + drop_range_name);
    drop_range_part->setColumns(storage_columns);
    drop_range_part->is_temp = true;

    /// XXX: Fill partition with default values to prevent coredump
    for (auto & type : metadata_snapshot->partition_key.data_types)
        drop_range_part->partition.value.push_back(type->getDefault());

    MergedBlockOutputStream out(
        drop_range_part,
        metadata_snapshot,
        storage_columns,
        {},
        getCompressionCodecForPart(0, drop_range_part->ttl_infos, time(nullptr)));

    out.writePrefix();
    /// out.write(metadata_snapshot->getSampleBlock());
    out.writeSuffixAndFinalizePart(drop_range_part);

    return drop_range_part;
}

bool StorageHaMergeTree::executeDropRange(HaQueueExecutingEntrySetPtr & executing_set)
{
    auto & entry = *executing_set->getExecuting();

    auto & drop_range_name = entry.new_part_name;
    auto drop_range_info = MergeTreePartInfo::fromPartName(drop_range_name, format_version);
    drop_range_info.mutation = MergeTreePartInfo::MAX_MUTATION; /// fix log created before adding mutation support /// TODO: REMOVE ME

    getContext()->getMergeList().cancelInPartition(getStorageID(), drop_range_info.partition_id, drop_range_info.max_block);

    LOG_DEBUG(log, "{} parts inside {}", (entry.detach ? "Detaching" : "Removing"), drop_range_name);

    auto metadata_snapshot = getInMemoryMetadataPtr();
    DataPartsVector parts_to_remove;

    if (drop_range_info.isFakeDropRangePart())
    {
        auto drop_range_part = createDropRangePart(drop_range_name, drop_range_info, metadata_snapshot);

        MergeTreeData::Transaction transaction(*this);
        parts_to_remove = renameTempPartAndReplace(drop_range_part, nullptr, &transaction);
        transaction.commit();
    }
    else
    {
        auto data_parts_lock = lockParts();
        parts_to_remove = removePartsInRangeFromWorkingSet(drop_range_info, true, data_parts_lock);
        if (parts_to_remove.empty())
        {
            LOG_INFO(log, "Try to execute {}, but no part is inside it. Will skip this log.", entry.toDebugString());
            return true;
        }
    }

    if (entry.detach)
    {
        /// If DETACH clone parts to detached/ directory
        for (auto & part : parts_to_remove)
        {
            LOG_INFO(log, "Detaching {}", part->relative_path);
            part->makeCloneInDetached("", metadata_snapshot);
        }

        LOG_DEBUG(log, "Detached {} parts inside {}.", parts_to_remove.size(), drop_range_name);
    }
    else
    {
        LOG_DEBUG(log, "Removed {} parts inside {}.", parts_to_remove.size(), drop_range_name);
    }

    /// We want to remove dropped parts from disk as soon as possible
    /// To be removed a partition should have zero refcount, therefore call the cleanup thread at exit
    parts_to_remove.clear();
    cleanup_thread.wakeup();

    return true;
}

bool StorageHaMergeTree::executeReplaceRange(HaQueueExecutingEntrySetPtr & executing_set)
{
    auto & entry = *executing_set->getExecuting();

    auto & drop_range_name = entry.replace_range_entry->drop_range_part_name;
    auto drop_range_info = MergeTreePartInfo::fromPartName(drop_range_name, format_version);
    drop_range_info.mutation = MergeTreePartInfo::MAX_MUTATION; /// fix log created before adding mutation support /// TODO: REMOVE ME

    auto metadata_snapshot = getInMemoryMetadataPtr();

    auto drop_range_part = createDropRangePart(drop_range_name, drop_range_info, metadata_snapshot);

    /// Fetch new parts from other replicas
    MutableDataPartsVector new_parts;
    ActiveDataPartSet fetched_parts(format_version);

    auto zookeeper = getZooKeeper();
    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(getContext());
    auto credentials = getContext()->getInterserverCredentials();
    String interserver_scheme = getContext()->getInterserverScheme();

    auto now = time(nullptr);

    for (auto & part_name : entry.replace_range_entry->new_part_names)
    {
        if (!fetched_parts.getContainingPart(part_name).empty())
            continue;

        bool all_success = false;
        auto candidate_replicas = log_exchanger.findActiveContainingPart(part_name, all_success);
        if (candidate_replicas.empty())
        {
            if (log_exchanger.isLostReplica(entry.source_replica))
                throw Exception(
                    "Part " + part_name + " is lost forever because no active replica has it and source replica is lost",
                    ErrorCodes::PART_IS_LOST_FOREVER);
            else if (all_success && entry.create_time + 1800 < now)
                throw Exception(
                    "Part " + part_name + " is lost forever because no active or failed replica has it", ErrorCodes::PART_IS_LOST_FOREVER);
            else
                throw Exception("No active replica has part " + part_name, ErrorCodes::NO_REPLICA_HAS_PART);
        }

        /// the "payload" field measures the load of the replica
        std::sort(candidate_replicas.begin(), candidate_replicas.end(), [](auto & lhs, auto & rhs) { return lhs.payload < rhs.payload; });
        auto & replica_to_fetch = candidate_replicas[0].replica;
        auto & part_to_fetch = candidate_replicas[0].containing_part;

        if (!fetched_parts.getContainingPart(part_to_fetch).empty())
            continue;

        String source_replica_path = zookeeper_path + "/replicas/" + replica_to_fetch;
        HaMergeTreeAddress address(zookeeper->get(source_replica_path + "/host"));
        if (interserver_scheme != address.scheme)
            throw Exception(
                "Interserver schemes are different '" + interserver_scheme + "' != '" + address.scheme + "', can't fetch part from "
                    + address.host,
                ErrorCodes::LOGICAL_ERROR);

        LOG_DEBUG(log, "Fetching part {} from {} for replacing", part_to_fetch, source_replica_path);

        auto part = fetcher.fetchPart(
            getInMemoryMetadataPtr(),
            getContext(),
            part_to_fetch,
            source_replica_path,
            address.host,
            address.replication_port,
            timeouts,
            credentials->getUser(),
            credentials->getPassword(),
            interserver_scheme,
            replicated_fetches_throttler,
            false, /// to_detached
            "tmp_replace_fetch_");

        LOG_DEBUG(log, "Fetched part {} from {} for replacing", part_to_fetch, source_replica_path);

        new_parts.push_back(std::move(part));
        fetched_parts.add(part_to_fetch);
    }

    DataPartsVector parts_to_remove;
    {
        auto data_parts_lock = lockParts();

        Transaction transaction(*this);
        renameTempPartAndReplace(drop_range_part, nullptr, &transaction, data_parts_lock, &parts_to_remove);
        for (auto & part : new_parts)
            renameTempPartAndReplace(part, nullptr, &transaction, data_parts_lock);
        transaction.commit(&data_parts_lock);
    }

    LOG_DEBUG(log, "Removed {} parts inside {} for replacing", parts_to_remove.size(), drop_range_name);

    parts_to_remove.clear();
    cleanup_thread.wakeup();

    return true;
}

void StorageHaMergeTree::executeMetadataAlter(const MutationEntry & entry)
{
    if (entry.alter_version <= metadata_version)
    {
        LOG_DEBUG(
            log, "Skip stalled mutation entry: {} with version {}, current: {}", entry.toString(), entry.alter_version, metadata_version);
        return;
    }

    LOG_DEBUG(log, "Execute : {} with version {}", entry.getNameForLogs(), entry.alter_version);

    auto columns_from_entry = ColumnsDescription::parse(entry.alter_info->columns_str);
    auto metadata_from_entry = HaMergeTreeTableMetadata::parse(entry.alter_info->metadata_str);

    auto zookeeper = getZooKeeper();

    Coordination::Requests requests;
    requests.emplace_back(zkutil::makeSetRequest(replica_path + "/columns", entry.alter_info->columns_str, -1));
    requests.emplace_back(zkutil::makeSetRequest(replica_path + "/metadata", entry.alter_info->metadata_str, -1));
    zookeeper->multi(requests);

    {
        auto lock = lockForAlter(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

        LOG_INFO(log, "Metadata changed in ZooKeeper. Applying changes locally.");

        auto metadata_diff = HaMergeTreeTableMetadata(*this, getInMemoryMetadataPtr()).checkAndFindDiff(metadata_from_entry);
        setTableStructure(std::move(columns_from_entry), metadata_diff);
        metadata_version = entry.alter_version;

        LOG_INFO(log, "Applied changes to the metadata of the table. Current metadata version: {}", metadata_version);
    }

    /// This transaction may not happen, but it's OK, because on the next retry we will eventually create/update this node
    zookeeper->createOrUpdate(replica_path + "/metadata_version", std::to_string(entry.alter_version), zkutil::CreateMode::Persistent);
    LOG_DEBUG(log, "Set metadata version to: {} ", metadata_version);
}

bool StorageHaMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & ) const
{
    return false;
    /// TODO:
    /// 1. for move
    /// 2. for drop part
    /// return queue.isVirtualPart(part);
}

void StorageHaMergeTree::queueUpdatingTask()
{
    try
    {
        queue.pullLogsToQueue(getZooKeeper(), queue_updating_task->getWatchCallback());
        queue_updating_task->scheduleAfter(getSettings()->ha_queue_update_sleep_ms);

        commitLogTask();
    }
    catch (const zkutil::KeeperException & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
        {
            restarting_thread.wakeup();
            return;
        }

        queue_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        queue_updating_task->scheduleAfter(QUEUE_UPDATE_ERROR_SLEEP_MS);
    }

}

void StorageHaMergeTree::commitLogTask()
{
    {
        /// Avoid frequent ZK ops
        auto curr_time = time(nullptr);
        if (static_cast<UInt64>(curr_time - last_commit_log_time) < getSettings()->ha_commit_log_period)
            return;
        last_commit_log_time = curr_time;
    }

    auto self_lsn_status = log_manager->getLSNStatus(true);
    if (self_lsn_status.committed_lsn == self_lsn_status.updated_lsn && self_lsn_status.committed_lsn != 0)
        return;

    bool all_success{false};
    auto lsn_status_list = log_exchanger.getLSNStatusList(&all_success);
    if (!all_success)
    {
        LOG_DEBUG(log, "Here are some failed replica, cannot commit LSN");
        return;
    }

    auto min_updated_lsn = self_lsn_status.updated_lsn;
    auto max_committed_lsn = self_lsn_status.committed_lsn;
    for (auto & lsn_status : lsn_status_list)
    {
        min_updated_lsn = std::min(min_updated_lsn, lsn_status.second.updated_lsn);
        max_committed_lsn = std::max(max_committed_lsn, lsn_status.second.committed_lsn);
    }
    if (max_committed_lsn > self_lsn_status.updated_lsn)
    {
        LOG_WARNING(log, "Self replica is oudated: updated_lsn {}, max_committed_lsn {} ", self_lsn_status.updated_lsn, max_committed_lsn);
    }


    /** Commit executed logs:
     *  Once some replica commit a LSN: we assume all replicas reach an agreement
     *  which the commited logs are executed by all replicas.
     *  Or there is someone lost which must be recovered by other mechanism instead of sync logs.
     *  Because the logs may be discards already
     */
    auto new_committed_lsn = std::max(min_updated_lsn, max_committed_lsn);
    if (self_lsn_status.committed_lsn < new_committed_lsn)
    {
        /// Forward compatibility, outdated commited_lsn/updated_lsn is tolerated.
        auto zookeeper = getZooKeeper();
        Coordination::Requests ops;
        ops.push_back(zkutil::makeSetRequest(getZKReplicaUpdatedLSNPath(), toString(self_lsn_status.updated_lsn), -1));
        ops.push_back(zkutil::makeSetRequest(getZKCommittedLSNPath(), toString(new_committed_lsn), -1));
        Coordination::Responses resps;
        zookeeper->tryMulti(ops, resps);

        log_manager->commitTo(new_committed_lsn);
        LOG_DEBUG(log, "Commit log on disk to LSN-{}", new_committed_lsn);
    }
}

void StorageHaMergeTree::forceSetTableStructure(zkutil::ZooKeeperPtr & zookeeper)
{
    auto lock = lockExclusively(RWLockImpl::NO_QUERY, getContext()->getSettingsRef().lock_acquire_timeout);

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

            sorting_key.recalculateWithNewAST(order_by_ast, new_metadata.columns, getContext());

            if (primary_key.definition_ast == nullptr)
            {
                /// Primary and sorting key become independent after this ALTER so we have to
                /// save the old ORDER BY expression as the new primary key.
                auto old_sorting_key_ast = old_metadata.getSortingKey().definition_ast;
                primary_key = KeyDescription::getKeyFromAST(
                    old_sorting_key_ast, new_metadata.columns, getContext());
            }
        }

        if (metadata_diff.sampling_expression_changed)
        {
            auto sample_by_ast = parse_key_expr(metadata_diff.new_sampling_expression);
            new_metadata.sampling_key.recalculateWithNewAST(sample_by_ast, new_metadata.columns, getContext());
        }

        if (metadata_diff.skip_indices_changed)
            new_metadata.secondary_indices = IndicesDescription::parse(metadata_diff.new_skip_indices, new_columns, getContext());

        if (metadata_diff.constraints_changed)
            new_metadata.constraints = ConstraintsDescription::parse(metadata_diff.new_constraints);

        if (metadata_diff.ttl_table_changed)
        {
            if (!metadata_diff.new_ttl_table.empty())
            {
                ParserTTLExpressionList parser;
                auto ttl_for_table_ast = parseQuery(parser, metadata_diff.new_ttl_table, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                new_metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                    ttl_for_table_ast, new_metadata.columns, getContext(), new_metadata.primary_key);
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
        auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, new_metadata.columns, getContext(), new_metadata.primary_key);
        new_metadata.column_ttls_by_name[name] = new_ttl_entry;
    }

    if (new_metadata.partition_key.definition_ast != nullptr)
        new_metadata.partition_key.recalculateWithNewColumns(new_metadata.columns, getContext());

    if (!metadata_diff.sorting_key_changed) /// otherwise already updated
        new_metadata.sorting_key.recalculateWithNewColumns(new_metadata.columns, getContext());

    /// Primary key is special, it exists even if not defined
    if (new_metadata.primary_key.definition_ast != nullptr)
    {
        new_metadata.primary_key.recalculateWithNewColumns(new_metadata.columns, getContext());
    }
    else
    {
        new_metadata.primary_key = KeyDescription::getKeyFromAST(new_metadata.sorting_key.definition_ast, new_metadata.columns, getContext());
        new_metadata.primary_key.definition_ast = nullptr;
    }

    if (!metadata_diff.sampling_expression_changed && new_metadata.sampling_key.definition_ast != nullptr)
        new_metadata.sampling_key.recalculateWithNewColumns(new_metadata.columns, getContext());

    if (!metadata_diff.skip_indices_changed) /// otherwise already updated
    {
        for (auto & index : new_metadata.secondary_indices)
            index.recalculateWithNewColumns(new_metadata.columns, getContext());
    }

    if (!metadata_diff.ttl_table_changed && new_metadata.table_ttl.definition_ast != nullptr)
        new_metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
            new_metadata.table_ttl.definition_ast, new_metadata.columns, getContext(), new_metadata.primary_key);

    /// Even if the primary/sorting/partition keys didn't change we must reinitialize it
    /// because primary/partition key column types might have changed.
    checkTTLExpressions(new_metadata, old_metadata);
    setProperties(new_metadata, old_metadata);

    auto table_id = getStorageID();
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(getContext(), table_id, new_metadata);
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

    LOG_DEBUG(log, "self_committed_lsn {} peer_updated_lsn {}", cloned_replica, self_committed_lsn, peer_updated_lsn);

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

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(getContext()->getSettingsRef());
    auto credentials = getContext()->getInterserverCredentials();

    auto part_names = fetcher.fetchPartList(
        partition_id,
        filter,
        source_path,
        replica_address.host,
        replica_address.replication_port,
        timeouts,
        credentials->getUser(),
        credentials->getPassword(),
        getContext()->getInterserverScheme());


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
        entry->new_part_name = part_name;
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
    /// TODO: String mimic_replica = getContext()->getMimicReplica(this->database_name, this->table_name);
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
            getContext()->getSchedulePool(),
            zookeeper_path + "/leader_election",
            *current_zookeeper,    /// current_zookeeper lives for the lifetime of leader_election,
                                   ///  since before changing `current_zookeeper`, `leader_election` object is destroyed in `partialShutdown` method.
            callback,
            replica_name,
            false);
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

bool StorageHaMergeTree::hasPartitionLevelTTL(const StorageInMemoryMetadata & metadata)
{
    if (!metadata.hasRowsTTL())
        return false;

    NameSet partition_columns(metadata.partition_key.column_names.begin(), metadata.partition_key.column_names.end());

    std::function<bool(const ASTPtr &)> isInPartitions = [&](const ASTPtr expr) -> bool {
        String name = expr->getAliasOrColumnName();
        if (partition_columns.count(name))
            return true;

        if (auto literal = expr->as<ASTLiteral>())
            return true;
        if (auto identifier = expr->as<ASTIdentifier>())
            return false;
        if (auto func = expr->as<ASTFunction>())
        {
            bool res = true;
            for (auto & arg : func->arguments->children)
                res &= isInPartitions(arg);
            return res;
        }
        return false;
    };

    return isInPartitions(metadata.table_ttl.rows_ttl.expression_ast);
}

void StorageHaMergeTree::tryExecutePartitionLevelTTL()
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    if (!hasPartitionLevelTTL(*metadata_snapshot))
        return;

    auto & partition_key_description = metadata_snapshot->partition_key;
    auto & rows_ttl = metadata_snapshot->table_ttl.rows_ttl;

    time_t now = time(nullptr);
    NameOrderedSet partitions_to_clean;

    const String * curr_partition = nullptr;
    auto data_parts =  getDataPartsVector();
    for (auto & part : data_parts)
    {
        if (curr_partition && *curr_partition == part->info.partition_id)
            continue;
        curr_partition = &part->info.partition_id;

        if (part->info.isFakeDropRangePart())
            continue;

        auto ttl = calcTTLForPartition(part->partition, partition_key_description, rows_ttl);
        if (ttl < now)
            partitions_to_clean.insert(*curr_partition);
    }

    if (partitions_to_clean.empty())
        return;

    std::ostringstream oss;
    oss << "Partitions have been expired:";
    for (auto & partition : partitions_to_clean)
        oss << " " << partition;
    LOG_DEBUG(log, oss.str());

    try
    {
        auto zookeeper = getZooKeeper();
        dropAllPartsInPartitions(
            zookeeper, partitions_to_clean, false, getContext()->getSettingsRef().replication_alter_partitions_sync > 0);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

time_t StorageHaMergeTree::calcTTLForPartition(
    const MergeTreePartition & partition, const KeyDescription & partition_key_description, const TTLDescription & ttl_description) const
{
    auto columns = partition_key_description.sample_block.cloneEmptyColumns();
    for (size_t i = 0; i < partition.value.size(); ++i)
        columns[i]->insert(partition.value[i]);

    auto block = partition_key_description.sample_block.cloneWithColumns(std::move(columns));
    ttl_description.expression->execute(block);

    auto & result_column_with_tn = block.getByName(ttl_description.result_column);
    auto & result_column = result_column_with_tn.column;
    auto & result_type = result_column_with_tn.type;

    if (isDate(result_type))
    {
        auto value = UInt16(result_column->getUInt(0));
        const auto & date_lut = DateLUT::instance();
        return date_lut.fromDayNum(DayNum(value));
    }
    else if (isDateTime(result_type))
    {
        return UInt32(result_column->getUInt(0));
    }
    else
    {
        throw Exception("Logical error in calcuate TTL value: unexpected TTL result column type", ErrorCodes::LOGICAL_ERROR);
    }
}

void StorageHaMergeTree::mergeSelectingTask()
{
    // Only the lead can merge parts in HA setting
    if (!is_leader)
        return;

    auto & pattern = getContext()->getSettingsRef().blacklist_for_merge_task_regex.value;
    if (std::regex_search(getStorageID().table_name, std::regex(pattern)))
    {
        LOG_TRACE(log, "Cancel merge task by settings blacklist_for_merge_task_regex: {} ", pattern);
        return;
    }

    const auto storage_settings = getSettings();

    SelectPartsDecision select_decision = SelectPartsDecision::NOTHING_TO_MERGE;
    LogEntry::Vec mutation_entries;
    bool any_exception = false;

    try
    {
        /// Try to remove TTL expired partitions before selecting parts to merge
        tryExecutePartitionLevelTTL();

        std::lock_guard merge_selecting_lock(merge_selecting_mutex);
        auto zookeeper = getZooKeeper();

        // The selection seems a bit complex here according to some facts:
        // - block_number is assigned sequentially per table (not per partition)
        // - snapshot in leader might not up to date.
        // To address this problem, the algorithm need to check its log queue and committed part list
        //

        // - decide which partition will be merged in this task
        // - decide candidate commited parts(not covered by other parts locally)  in this partition
        // we should prefer to choose continuous parts. if not
        // The closest parts's gap should be checked based on logs.
        //
        //
        // To check if two part [min_1, max_1], [min_2, max_2] in partition p could merge, we need to
        // guarantee the block_number(gap) between max_1 and min_2 has not been assigned to p, and not
        // ready in leader yet. How to address without race condition is quite complex.
        //
        // Look through the log queue + delay selection strategy should be applicable in practice.
        auto merge_pred = queue.getMergePredicate(zookeeper);
        auto merges_and_mutations_queued = queue.countMergesAndPartMutations();


        /// select parts to merge
        do
        {
            /// if leader A generates some merge logs and then crash, the new leader B may not be able to execute those merges.
            /// In order to let B continue to make progress on merges, use `merges_of_self` in concurrency check below
            size_t num_queued_merges = merges_and_mutations_queued.merges_of_self;
            size_t max_queued_merges = storage_settings->max_replicated_merges_in_queue;

            if (num_queued_merges >= max_queued_merges)
            {
                LOG_TRACE(
                    log,
                    "Number of queued self merges ({}) is greater than max_replicated_merges_in_queue ({}), so won't select new parts to "
                    "merge",
                    num_queued_merges,
                    max_queued_merges);
                break;
            }

            size_t max_source_parts_size = merger_mutator.getMaxSourcePartsSizeForMerge(max_queued_merges, num_queued_merges);
            if (max_source_parts_size == 0)
            {
                LOG_DEBUG(log, "max_source_parts_size is 0, won't select new parts to merge");
                break;
            }

            Stopwatch watch;

            std::vector<FutureMergedMutatedPart> future_merge_parts;
            select_decision = merger_mutator.selectPartsToMergeMulti(
                future_merge_parts,
                false, // aggressive
                max_source_parts_size,
                merge_pred,
                false, // merge_with_ttl_allowed
                nullptr // out_disable_reason
            );

            if (auto elapsed = watch.elapsedMilliseconds(); elapsed > 1000)
                LOG_DEBUG(log, "Selected {} groups of parts to merge in {} ms.", future_merge_parts.size(), elapsed);

            if (SelectPartsDecision::SELECTED == select_decision)
                createLogEntriesToMergeParts(future_merge_parts); /// Will throw exception on any error

        } while (false);

        /// select parts to mutate
        do
        {
            if (queue.countUnfinishedMutations(/*include_alter_metadata=*/false) == 0)
                break;

            size_t num_queued_mutates = merges_and_mutations_queued.mutations_of_self;
            size_t max_queued_mutates = getSettings()->max_replicated_mutations_in_queue;

            if (num_queued_mutates >= max_queued_mutates)
            {
                LOG_TRACE(
                    log,
                    "Number of queued self mutates ({}) is greater than max_replicated_mutations_in_queue ({}), so won't select new parts "
                    "to merge",
                    num_queued_mutates,
                    max_queued_mutates);
                break;
            }
            size_t max_parts_to_mutate = max_queued_mutates - num_queued_mutates;

            /// Choose parts to mutate.
            DataPartsVector data_parts = getDataPartsVector();
            for (const auto & part : data_parts)
            {
                auto desired_mutation_version = merge_pred.getDesiredMutationVersion(part);
                if (!desired_mutation_version)
                    continue;

                auto entry = createLogEntryToMutatePart(part->info, desired_mutation_version->first, desired_mutation_version->second);
                mutation_entries.push_back(std::move(entry));
                if (mutation_entries.size() >= max_parts_to_mutate)
                    break;
            }

            if (!mutation_entries.empty())
                queue.write(mutation_entries);

        } while (false);
    }
    catch (...)
    {
        any_exception = true;
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    if (any_exception)
        merge_selecting_task->scheduleAfter(MERGE_SELECTING_SLEEP_MS);
    else if (SelectPartsDecision::SELECTED != select_decision && mutation_entries.size() == 0)
        merge_selecting_task->scheduleAfter(MERGE_SELECTING_SLEEP_MS);
    else
        merge_selecting_task->schedule();
}

void StorageHaMergeTree::mutationsFinalizingTask()
{
    bool needs_reschedule = false;

    auto settings = getSettings();

    try
    {
        needs_reschedule = queue.tryFinalizeMutations(getZooKeeper());

        time_t now = time(nullptr);
        if (last_check_hang_mutation_time + settings->ha_check_hang_mutations_interval < UInt64(now))
        {
            last_check_hang_mutation_time = now;
            checkAndFixHangMutation();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        needs_reschedule = true;
    }

    if (needs_reschedule)
    {
        mutations_finalizing_task->scheduleAfter(MUTATIONS_FINALIZING_SLEEP_MS);
    }
    else
    {
        /// Even if no mutations seems to be done or appeared we are trying to
        /// finalize them in background because manual control the launch of
        /// this function is error prone. This can lead to mutations that
        /// processed all the parts but have is_done=0 state for a long time. Or
        /// killed mutations, which are also considered as undone.
        mutations_finalizing_task->scheduleAfter(MUTATIONS_FINALIZING_IDLE_SLEEP_MS);
    }
}

/// Under some rare situations, replicas may contain conflict part lists. e.g.,
///   replica 1: all_1_1_0 all_2_3_1
///   replica 2: all_1_2_1 all_3_3_0
/// In this case, mutate logs generated from the leader's parts may not cover parts on the other replica.
/// As a result, mutation on the non-leader replica may hang forever.
/// This function tries to detect such anomaly and fix it by creating logs that will mutate local parts.
void StorageHaMergeTree::checkAndFixHangMutation()
{
    /// only non-leader replica needs to check for hang mutation
    /// because leader replica will create mutate logs based on the parts it have
    if (is_leader)
        return;

    auto settings = getSettings();

    /// block merge select task during the execution of this task
    /// because we may add part mutate log
    std::lock_guard merge_selecting_lock(merge_selecting_mutex);

    size_t num_queued_mutates = queue.countMergesAndPartMutations().mutations_of_self;
    size_t max_queued_mutates = settings->max_replicated_mutations_in_queue;
    if (num_queued_mutates >= max_queued_mutates)
    {
        LOG_TRACE(log, "Skip hang mutation check because reached max_replicated_mutations_in_queue");
        return;
    }
    size_t max_parts_to_fix = max_queued_mutates - num_queued_mutates;

    HaMergeTreeMutationEntryPtr mutation = queue.getMutationForHangCheck();
    if (!mutation)
        return;

    /// check whether any replica has finished the mutation
    String finished_replica;
    std::map<String, GetMutationStatusResponse> responses = log_exchanger.getMutationStatus(mutation->znode_name);
    for (auto & [replica, response] : responses)
    {
        if (replica == replica_name)
            continue;
        if (response.is_done || response.mutation_pointer >= mutation->znode_name)
        {
            finished_replica = replica;
            break;
        }
    }

    if (finished_replica.empty())
        return; /// can't hang because no replica has finished the execution

    auto & finished_replica_status = responses[finished_replica];
    time_t now = time(nullptr);
    if (finished_replica_status.finish_time + settings->ha_check_hang_mutations_wait_period > UInt64(now))
        return; /// still in the wait period

    auto zookeeper = getZooKeeper();
    /// Check whether the mutation was killed. If so, the queue will remove it later
    if (!finished_replica_status.is_found && !zookeeper->exists(zookeeper_path + "/mutations/" + mutation->znode_name))
    {
        LOG_DEBUG(log, "The mutation {} was killed, skip the hang check", mutation->znode_name);
        return;
    }

    LOG_DEBUG(log, "Checking whether mutation {} might hang or not", mutation->getNameForLogs());

    queue.pullLogsToQueue(zookeeper, {}, /*ignore_backoff=*/true);

    Strings parts_to_check = queue.getPartsToMutateWithoutLogs(mutation->znode_name);
    if (parts_to_check.empty())
        return;

    LOG_DEBUG(
        log,
        "{} has already finished mutation {}, but current replica still has {} parts to mutate without logs. Check whether the mutation is "
        "hang.",
        finished_replica,
        mutation->getNameForLogs(),
        parts_to_check.size());

    Strings parts_to_fix;

    /// Check whether the part needs fix or not
    Strings request;
    size_t request_batch_size = 10;
    for (size_t i = 0; i < parts_to_check.size(); ++i)
    {
        request.push_back(parts_to_check[i]);
        if (request.size() == request_batch_size || i == parts_to_check.size() - 1)
        {
            auto containing_parts = log_exchanger.findActiveContainingParts(finished_replica, request);
            for (size_t j = 0; j < containing_parts.size(); ++j)
            {
                auto & part = request[j];
                auto & containing_part = containing_parts[j];

                /// if the finished replica has an addable containing part, no need to fix because
                /// the part can be processed by replaying logs
                /// TODO:
                /// if (!containing_part.empty())
                /// {
                ///     auto lock = lockParts();
                ///     if (metastore->checkPartNameAddable(containing_part, lock))
                ///         continue;
                /// }
                /// otherwise the mutation may hang due to conflict part list, and we must fix the part
                String reason;
                if (containing_part.empty())
                    reason = "no containing part on replica " + finished_replica;
                else
                    reason = "containing part " + containing_part + " is not addable";
                LOG_INFO(log, "need to fix part {} : {}", part, reason);

                parts_to_fix.push_back(part);
                if (parts_to_fix.size() >= max_parts_to_fix)
                    break;
            }
            request.clear();
        }

        if (parts_to_fix.size() >= max_parts_to_fix)
            break;
    }

    if (parts_to_fix.empty())
    {
        LOG_DEBUG(log, "all parts have an addable containing part, no need to fix mutation");
        return;
    }

    LogEntry::Vec log_entries;
    for (String & part : parts_to_fix)
    {
        auto info = MergeTreePartInfo::fromPartName(part, format_version);
        auto log_entry = createLogEntryToMutatePart(info, mutation->block_number, mutation->alter_version, /*current_replica_only=*/true);
        log_entries.push_back(log_entry);
    }
    queue.write(log_entries, /*broadcast=*/false);
    LOG_INFO(log, "Wrote {} logs to fix hang mutation {}", log_entries.size(), mutation->getNameForLogs());

    background_executor.triggerTask();
}

void StorageHaMergeTree::createLogEntriesToMergeParts(const std::vector<FutureMergedMutatedPart> & future_parts)
{
    LogEntry::Vec entries;

    for (auto & future_part : future_parts)
    {
        auto entry = std::make_shared<LogEntry>();
        entry->type = LogEntry::MERGE_PARTS;
        entry->source_replica = replica_name;
        for (auto & part : future_part.parts)
            entry->source_parts.push_back(part->name);
        entry->new_part_name = future_part.name;
        entry->lsn = allocateLSN();
        entry->create_time = time(nullptr);
        entries.push_back(std::move(entry));
    }

    queue.write(entries);
}

HaMergeTreeLogEntryPtr StorageHaMergeTree::createLogEntryToMutatePart(
    const MergeTreePartInfo & part_info, Int64 mutation_version, int alter_version, bool current_replica_only)
{
    MergeTreePartInfo new_part_info = part_info;
    new_part_info.mutation = mutation_version;

    auto entry = std::make_shared<LogEntry>();
    entry->type = LogEntry::MUTATE_PART;
    entry->source_replica = replica_name;
    entry->source_parts.push_back(part_info.getPartName());
    entry->new_part_name = new_part_info.getPartName();
    entry->lsn = allocLSNAndSet();
    entry->create_time = time(nullptr);
    entry->alter_version = alter_version;
    if (current_replica_only)
        entry->from_replica = replica_name;
    return entry;
}

void StorageHaMergeTree::dropAllPartsInPartitions(zkutil::ZooKeeperPtr & zookeeper, const NameOrderedSet & partitions, bool detach, bool sync)
{
    /// It's OK to use same block id
    auto block_id = allocateBlockNumberDirect(zookeeper);

    LogEntry::Vec entries;
    entries.reserve(partitions.size());

    for (auto & partition : partitions)
    {
        entries.push_back(std::make_shared<LogEntry>());
        auto & entry = entries.back();

        MergeTreePartInfo drop_range_info(partition, 0, block_id, MergeTreePartInfo::MAX_LEVEL, MergeTreePartInfo::MAX_MUTATION);
        entry->type = LogEntry::DROP_RANGE;
        entry->source_replica = replica_name;
        entry->new_part_name = drop_range_info.getPartName();
        entry->detach = detach;
        entry->lsn = allocateLSN();
        entry->create_time = time(nullptr);
    }

    queue.write(entries);

    if (sync)
    {
        /// It's important to acquire CurrentlyExecuting before executing them right now.
        /// Or queueTask() might select and execute concurrently.
        /// NEED NOT lock here, creating CurrentlyExecuting do not change queue state
        std::vector<HaQueueExecutingEntrySetPtr> executing_set_list;
        executing_set_list.reserve(entries.size());
        for (auto & entry : entries)
            executing_set_list.push_back(queue.tryCreateExecutingSet(entry));

        LogEntry::Vec entries_to_remove;

        /// If a exception thrown here, CurreentlyExecuting would reset entries so that queueTask() would retry later.
        for (auto & executing_set : executing_set_list)
        {
            if (executing_set)
            {
                executeDropRange(executing_set);
                entries_to_remove.push_back(executing_set->getExecuting());
            }
        }

        queue.removeProcessedEntries(entries_to_remove);
    }
}

void StorageHaMergeTree::dropPartNoWaitNoThrow(const String & part_name)
{
    assertNotReadonly();
    if (!is_leader)
        throw Exception("DROP PART cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    auto zookeeper = getZooKeeper();
    auto entry = std::make_shared<LogEntry>();
    constexpr bool detach = false;
    constexpr bool throw_if_noop = true;

    dropPartImpl(zookeeper, part_name, *entry, detach, throw_if_noop);
}

void StorageHaMergeTree::dropPart(const String & part_name, bool detach, ContextPtr query_context)
{
    auto & settings = query_context->getSettingsRef();

    assertNotReadonly();
    if (!is_leader && !settings.ignore_leader_check)
        throw Exception("DROP PART cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    auto zookeeper = getZooKeeper();
    auto entry = std::make_shared<LogEntry>();
    constexpr bool throw_if_noop = true;

    dropPartImpl(zookeeper, part_name, *entry, detach, throw_if_noop);

    if (settings.replication_alter_partitions_sync > 0)
    {
        if (auto executing_set = queue.tryCreateExecutingSet(entry))
        {
            executeDropRange(executing_set);
            queue.removeProcessedEntry(entry);
        }
    }
}

bool StorageHaMergeTree::dropPartImpl(zkutil::ZooKeeperPtr & zookeeper, const String & part_name, LogEntry & entry, bool detach, bool throw_if_noop)
{
    LOG_TRACE(log, "Will try to insert a log entry to DROP_RANGE for part: " + part_name);

    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

    auto part = getPartIfExists(part_info, {MergeTreeDataPartState::Committed});

    if (!part)
    {
        if (throw_if_noop)
            throw Exception("Part " + part_name + " not found locally, won't try to drop it.", ErrorCodes::NO_SUCH_DATA_PART);
        return false;
    }

    /// impl hasDropRange
    /// TODO:

    /// There isn't a lot we can do otherwise. Can't cancel merges because it is possible that a replica already
    /// finished the merge.
    if (partIsAssignedToBackgroundOperation(part))
    {
        if (throw_if_noop)
            throw Exception("Part " + part_name
                            + " is currently participating in a background operation (mutation/merge)"
                            + ", try again later", ErrorCodes::PART_IS_TEMPORARILY_LOCKED);
        return false;
    }

    entry.type = LogEntry::DROP_RANGE;
    entry.source_replica = replica_name;
    entry.new_part_name = part_name;
    entry.create_time = time(nullptr);
    entry.block_id = allocateBlockNumberDirect(zookeeper);
    entry.lsn = allocateLSN();
    entry.detach = detach;

    queue.write(entry);

    return true;
}

void StorageHaMergeTree::clearEmptyParts()
{
    auto settings = getSettings();
    if (!settings->remove_empty_parts)
        return;

    time_t now = time(nullptr);
    time_t drop_ranges_lifetime = std::max(settings->drop_ranges_lifetime.totalSeconds(), settings->old_parts_lifetime.totalSeconds() * 2);
    bool is_leader_value = this->is_leader;

    DataPartsVector drop_range_parts;
    auto parts = getDataPartsVector();


    for (const auto & part : parts)
    {
        if (part->info.isFakeDropRangePart() && now - part->modification_time > drop_ranges_lifetime)
            drop_range_parts.push_back(part);

        if (!part->info.isFakeDropRangePart() && part->rows_count == 0 && is_leader)
            dropPartNoWaitNoThrow(part->name);
    }

    if (drop_range_parts.size() >= settings->min_drop_ranges_to_enable_cleanup)
    {
        auto data_parts_lock = lockParts();
        removePartsFromWorkingSet(drop_range_parts, true, data_parts_lock);

        for (auto & part : drop_range_parts)
            LOG_DEBUG(log, "Remove drop range part from filesystem {}", part->name);
    }
}

// Partition helpers
void StorageHaMergeTree::dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context)
{
    auto & settings = query_context->getSettingsRef();

    String partition_id = getPartitionIDFromQuery(partition, query_context);

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();
    dropAllPartsInPartitions(zookeeper, {partition_id}, detach, settings.replication_alter_partitions_sync > 0);
}

void StorageHaMergeTree::dropPartitionWhere(const ASTPtr & predicate, bool detach, ContextPtr query_context)
{
    auto & settings = query_context->getSettingsRef();

    assertNotReadonly();
    if (!is_leader && !settings.ignore_leader_check)
        throw Exception("DROP PARTITION WHERE cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    auto parts = getPartsByPredicate(predicate);

    zkutil::ZooKeeperPtr zookeeper = getZooKeeper();
    dropAllPartsInPartitions(
        zookeeper, collectPartitionsFromParts(parts), detach, settings.replication_alter_partitions_sync > 0);
}

PartitionCommandsResultInfo StorageHaMergeTree::attachPartition(
    const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool attach_part, ContextPtr query_context)
{
    assertNotReadonly();

    PartitionCommandsResultInfo results;
    PartsTemporaryRename renamed_parts(*this, "detached/");
    MutableDataPartsVector loaded_parts = tryLoadPartsToAttach(partition, attach_part, query_context, renamed_parts);

    /// TODO Allow to use quorum here.
    HaMergeTreeBlockOutputStream output(*this, metadata_snapshot, query_context);

    Names old_names;
    for (auto & part : loaded_parts)
        old_names.push_back(part->name);

    output.writeExistingParts(loaded_parts);

    renamed_parts.old_and_new_names.clear();

    LOG_DEBUG(log, "Attached {} parts", loaded_parts.size());
    for (size_t i = 0; i < loaded_parts.size(); ++i)
    {
        LOG_TRACE(log, "Attached part {} as {}", old_names[i], loaded_parts[i]->name);

        results.push_back(PartitionCommandResultInfo{
            .partition_id = loaded_parts[i]->info.partition_id,
            .part_name = loaded_parts[i]->name,
            .old_part_name = old_names[i],
        });
    }

    return results;
}

void StorageHaMergeTree::replacePartitionFrom(
    const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr query_context)
{
    /// First argument is true, because we possibly will add new data to current table.
    auto lock1 = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
    auto lock2 = source_table->lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);

    auto source_metadata_snapshot = source_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    Stopwatch watch;
    MergeTreeData & src_data = checkStructureAndGetMergeTreeData(source_table, source_metadata_snapshot, metadata_snapshot);
    String partition_id = getPartitionIDFromQuery(partition, query_context);

    /// NOTE: Some covered parts may be missing in src_all_parts if corresponding log entries are not executed yet.
    DataPartsVector src_all_parts = src_data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    DataPartsVector src_parts;
    MutableDataPartsVector dst_parts;

    LOG_DEBUG(log, "Cloning {} parts", src_all_parts.size());

    static const String TMP_PREFIX = "tmp_replace_from_";
    auto zookeeper = getZooKeeper();

    /// TODO:
    /// String alter_partition_version_path = zookeeper_path + "/alter_partition_version";
    /// Coordination::Stat alter_partition_version_stat;
    /// zookeeper->get(alter_partition_version_path, &alter_partition_version_stat);

    for (const auto & src_part : src_all_parts)
    {
        if (src_part->info.isFakeDropRangePart())
            continue;

        /// We also make some kind of deduplication to avoid duplicated parts in case of ATTACH PARTITION
        /// Assume that merges in the partition are quite rare
        /// Save deduplication block ids with special prefix replace_partition

        if (!canReplacePartition(src_part))
            throw Exception(
                "Cannot replace partition '" + partition_id + "' because part '" + src_part->name
                    + "' has inconsistent granularity with table",
                ErrorCodes::LOGICAL_ERROR);

        /// This will generate unique name in scope of current server process.
        Int64 temp_index = insert_increment.get();
        MergeTreePartInfo dst_part_info(partition_id, temp_index, temp_index, src_part->info.level);
        auto dst_part = cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, metadata_snapshot);

        src_parts.emplace_back(src_part);
        dst_parts.emplace_back(dst_part);
    }

    try
    {
        /// Allocate blocks in batch ahead
        String block_numbers_prefix = zookeeper_path + "/block_numbers/block-";
        size_t block_numbers_prefix_len = block_numbers_prefix.length();
        Coordination::Requests ops;
        for (size_t i = 0; i < dst_parts.size() + 1; ++i) /// + 1 for drop_range
            ops.push_back(zkutil::makeCreateRequest(block_numbers_prefix, "", zkutil::CreateMode::EphemeralSequential));
        Coordination::Responses results = zookeeper->multi(ops);

        auto get_block_id = [&](size_t i) {
            return dynamic_cast<Coordination::CreateResponse &>(*results[i]).path_created.substr(block_numbers_prefix_len);
        };

        /// Create drop range part
        String drop_range_block_id = get_block_id(0);
        MergeTreePartInfo drop_range_info(
            partition_id, 0, parse<Int64>(drop_range_block_id), MergeTreePartInfo::MAX_LEVEL, MergeTreePartInfo::MAX_MUTATION);
        auto drop_range_name = drop_range_info.getPartName();
        auto drop_range_part = createDropRangePart(drop_range_name, drop_range_info, metadata_snapshot);

        /// Create REPLACE_RANGE entry
        auto entry = std::make_shared<LogEntry>();
        entry->block_id = drop_range_block_id;
        entry->type = HaMergeTreeLogEntry::REPLACE_RANGE;
        entry->source_replica = replica_name;
        entry->lsn = allocateLSN();
        entry->create_time = time(nullptr);
        entry->is_executed = true;
        entry->replace_range_entry = std::make_shared<HaMergeTreeLogEntry::ReplaceRangeEntry>();

        /// Fill drop range name
        entry->replace_range_entry->drop_range_part_name = drop_range_name;

        /// Fill new part names
        for (size_t i = 0; i < dst_parts.size(); ++i)
        {
            auto part_block_number = parse<Int64>(get_block_id(i + 1));

            auto & part = dst_parts[i];
            part->info.min_block = part_block_number;
            part->info.max_block = part_block_number;
            part->info.level = 0;
            part->name = part->getNewName(part->info);

            entry->replace_range_entry->new_part_names.push_back(part->name);
        }

        queue.write(entry);

        /// Commit drop range part and new parts
        DataPartsVector parts_to_remove;
        {
            auto data_parts_lock = lockParts();

            Transaction transaction(*this);
            renameTempPartAndReplace(drop_range_part, nullptr, &transaction, data_parts_lock, &parts_to_remove);
            for (auto & part : dst_parts)
                renameTempPartAndReplace(part, nullptr, &transaction, data_parts_lock);
            transaction.commit(&data_parts_lock);
        }

        LOG_DEBUG(log, "Removed {} parts inside {} for replacing", parts_to_remove.size(), drop_range_name);

        parts_to_remove.clear();
        cleanup_thread.wakeup();

        PartLog::addNewParts(getContext(), dst_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(getContext(), dst_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
        throw;
    }
}

void StorageHaMergeTree::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr query_context)
{
    throw Exception("not supported", ErrorCodes::NOT_IMPLEMENTED);
}

bool StorageHaMergeTree::checkIfDetachedPartitionExists(const String & partition_name)
{
    fs::directory_iterator dir_end;
    for (const std::string & path : getDataPaths())
    {
        for (fs::directory_iterator dir_it{fs::path(path) / "detached/"}; dir_it != dir_end; ++dir_it)
        {
            MergeTreePartInfo part_info;
            if (MergeTreePartInfo::tryParsePartName(dir_it->path().filename(), &part_info, format_version) && part_info.partition_id == partition_name)
                return true;
        }
    }
    return false;
}

void StorageHaMergeTree::fetchPartition(
    const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, const String & from, bool fetch_part, ContextPtr query_context)
{
    assertNotReadonly();

    /// Macros::MacroExpansionInfo info;
    /// info.expand_special_macros_only = false; //-V1048
    /// info.table_id = getStorageID();
    /// info.table_id.uuid = UUIDHelpers::Nil;
    /// auto expand_from = query_context->getMacros()->expand(from_, info);
    /// String auxiliary_zookeeper_name = extractZooKeeperName(expand_from);
    /// String from = extractZooKeeperPath(expand_from);
    /// if (from.empty())
    ///     throw Exception("ZooKeeper path should not be empty", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    /// zkutil::ZooKeeperPtr zookeeper;
    /// if (auxiliary_zookeeper_name != default_zookeeper_name)
    ///     zookeeper = getContext()->getAuxiliaryZooKeeper(auxiliary_zookeeper_name);
    /// else
    ///     zookeeper = getZooKeeper();

    /// if (from.back() == '/')
    ///     from.resize(from.size() - 1);

    if (fetch_part)
    {
        String part_name = partition->as<ASTLiteral &>().value.safeGet<String>();
        /// TODO handle exception
        fetchPart(nullptr, part_name, zookeeper_path + "/replicas/" + from, false);
        return;
    }

    String partition_id = getPartitionIDFromQuery(partition, query_context);
    /// LOG_INFO(log, "Will fetch partition {} from shard {} (zookeeper '{}')", partition_id, from_, auxiliary_zookeeper_name);

    fetchPartitionImpl(partition_id, {}, from, query_context);
}

void StorageHaMergeTree::fetchPartitionWhere(
    const ASTPtr & predicate, const StorageMetadataPtr & metadata_snapshot, const String & from, ContextPtr query_context)
{
    assertNotReadonly();

    String filter = serializeAST(*predicate, true);
    fetchPartitionImpl({}, filter, from, query_context);
}

void StorageHaMergeTree::fetchPartitionImpl(
    const String & partition_id, const String & filter, const String & from, ContextPtr query_context)
{
    String from_replica_path = zookeeper_path + "/replicas/" + from;
    auto from_replica_address = getReplicaAddress(from);

    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(getContext()->getSettingsRef());
    auto credentials = getContext()->getInterserverCredentials();

    auto part_names = fetcher.fetchPartList(
        partition_id,
        filter,
        from_replica_path,
        from_replica_address.host,
        from_replica_address.replication_port,
        timeouts,
        credentials->getUser(),
        credentials->getPassword(),
        getContext()->getInterserverScheme());

    for (auto & part_name : part_names)
    {
        auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
        if (getActiveContainingPart(part_info))
            continue;

        try
        {
            bool fetched = fetchPart(nullptr, part_name, from_replica_path, false);
            if (!fetched)
                LOG_ERROR(log, "Failed to fetch part {} from {}", part_name, from_replica_path);
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log, e.displayText());
        }
    }
}

void StorageHaMergeTree::movePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, ContextPtr query_context)
{
    auto * merge_tree_table = dynamic_cast<MergeTreeData *>(source_table.get());
    if (!merge_tree_table)
        throw Exception("Target table must be MergeTree family for Move Partition.", ErrorCodes::LOGICAL_ERROR);

    if(source_table->getStoragePolicy()->getName() != getStoragePolicy()->getName())
        throw Exception("Table must have same storage_policy for Move partition", ErrorCodes::LOGICAL_ERROR);

    String partition_id = getPartitionIDFromQuery(partition, query_context);

    /// copy all the list in fromtbl's detached directory to table's detached directory
    for (const auto & disk : merge_tree_table->getStoragePolicy()->getDisks())
    {
        bool found_corresponding_disk = false;
        for (auto & target_disk : getStoragePolicy()->getDisks())
        {
            if (target_disk->getName() == disk->getName())
            {
                found_corresponding_disk = true;
                break;
            }
        }
        if (!found_corresponding_disk)
            throw Exception(
                "Cannot move partition from " + source_table->getStorageID().getNameForLogs() + " because cannot found corresponding disk",
                ErrorCodes::BAD_ARGUMENTS);

        auto source_detached_path = fs::path(merge_tree_table->getRelativeDataPath()) / "detached/";
        auto target_detached_path = fs::path(getRelativeDataPath()) / "detached/";

        for (auto it = disk->iterateDirectory(source_detached_path); it->isValid(); it->next())
        {
            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(it->name(), &part_info, format_version) || part_info.partition_id != partition_id)
                continue;

            auto source_part_path = source_detached_path / it->name();
            auto target_part_path = target_detached_path / it->name();

            if (disk->exists(target_part_path))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Partition {} already exists in {} for table {}",
                    partition_id,
                    target_detached_path.string(),
                    getStorageID().getNameForLogs());

            disk->moveFile(source_part_path, target_part_path);

            LOG_DEBUG(log, "Moved part from {} to {} in disk [{}] {}", source_part_path.string(), target_part_path.string(), disk->getName(), disk->getPath());
        }
    }

    /// TODO: should we detect conflicts ahead ?
}

MutationCommands StorageHaMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & ) const
{
    return {};
    /// TODO: return queue.getFirstAlterMutationCommandsForPart(part);
}

void StorageHaMergeTree::startBackgroundMovesIfNeeded()
{
    if (areBackgroundMovesNeeded())
        background_moves_executor.start();
}

std::unique_ptr<MergeTreeSettings> StorageHaMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getReplicatedMergeTreeSettings());
}

bool StorageHaMergeTree::isReplicaLostOrOffline(zkutil::ZooKeeperPtr & zookeeper, const String & replica) const
{
    String replica_is_lost = zookeeper->get(zookeeper_path + "/replicas/" + replica + "/is_lost");
    if (replica_is_lost == "1")
        return true;

    /// TODO:
    return false;
    /// String replica_is_offline;
    /// if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/is_offline", replica_is_offline))
    ///     return false;
    /// return replica_is_offline == "1";
}

void StorageHaMergeTree::systemExecuteLog(const std::vector<UInt64> & lsns)
{
    auto entries = queue.lookupEntriesByLSN(lsns);
    for (auto & entry : entries)
    {
        auto executing_set = queue.tryCreateExecutingSet(entry);
        if (!executing_set)
            continue;

        try
        {
            if (executeLogEntry(executing_set))
            {
                LOG_DEBUG(log, "system execute log succeed: {} ", entry->toDebugString());
                queue.removeProcessedEntry(entry);
            }
            else
                LOG_ERROR(log, "system execute log failed: {} ", entry->toDebugString());
        }
        catch (...)
        {
            LOG_ERROR(log, "system execute log failed: {}: {} ", entry->toDebugString(), getCurrentExceptionMessage(true));
        }
    }
}

void StorageHaMergeTree::systemSkipLog(const std::vector<UInt64> & lsns)
{
    auto entries = queue.lookupEntriesByLSN(lsns);
    queue.removeProcessedEntries(entries);
}

void StorageHaMergeTree::systemSetValues(const ASTPtr & values_changes)
{
    auto & changes = values_changes->as<ASTSetQuery &>().changes;

    for (auto & c : changes)
    {
        if (c.name == "committed_lsn")
        {
            log_manager->commitTo(c.value.get<UInt64>());
        }
        else
        {
            throw Exception("Unknown value: " + c.name, ErrorCodes::BAD_ARGUMENTS);
        }
    }
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
    String block_numbers_path_prefix = zookeeper_path + "/block_numbers/block-";

    auto block_path = zookeeper->create(block_numbers_path_prefix, "", zkutil::CreateMode::EphemeralSequential);

    auto block_number = parse<UInt64>(block_path.substr(block_numbers_path_prefix.length()));
    return block_number;
}

void StorageHaMergeTree::writeMutationLog(MutationLogElement::Type type, const MutationEntry & mutation_entry)
try
{
    auto log = getContext()->getMutationLog();
    if (!log)
        return;

    auto storage_id = getStorageID();

    MutationLogElement elem;
    elem.event_type = type;
    elem.event_time = time(nullptr);
    elem.database_name = storage_id.database_name;
    elem.table_name = storage_id.table_name;
    elem.mutation_id = mutation_entry.znode_name;
    elem.query_id = mutation_entry.query_id;
    elem.create_time = mutation_entry.create_time;
    elem.block_number = mutation_entry.block_number;
    for (const MutationCommand & command : mutation_entry.commands)
    {
        WriteBufferFromOwnString out;
        formatAST(*command.ast, out, false, true);
        elem.commands.push_back(out.str());
    }

    log->add(elem);
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

#pragma clang diagnostic pop
