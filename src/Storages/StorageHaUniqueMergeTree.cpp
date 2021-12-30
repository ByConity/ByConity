#include <Storages/StorageHaUniqueMergeTree.h>

// #include <DataStreams/InsertPartToViews.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
// #include <Storages/MergeTree/BitEngineDictionary.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/MergeTree/HaMergeTreeReplicaEndpoint.h>
#include <Storages/MergeTree/HaUniqueMergeTreeBlockOutputStream.h>
#include <Storages/PartitionCommands.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/StorageHaUniqueMergeTree.h>
#include <Storages/MutationCommands.h>
#include <Parsers/queryToString.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Poco/DirectoryIterator.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"

namespace CurrentMetrics
{
    extern const Metric LeaderReplica;
    extern const Metric BackgroundUniqueTableSchedulePoolTask;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int ALL_REPLICAS_LOST;
    //    extern const int BAD_ARGUMENTS;
    //    extern const int BAD_DATA_PART_NAME;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int INCORRECT_DATA;
//    extern const int LOGICAL_ERROR;
//    extern const int NOT_FOUND_NODE;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int NO_REPLICA_HAS_PART;
    extern const int NO_ZOOKEEPER;
//    extern const int PARTITION_ALREADY_EXISTS;
//    extern const int PART_IS_LOST_FOREVER;
//    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int READONLY;
    extern const int REPLICA_IS_ALREADY_EXIST;
    extern const int REPLICA_STATUS_CHANGED;
//    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int TOO_FEW_LIVE_REPLICAS;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
//    extern const int UNFINISHED;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int NO_SUCH_DATA_PART;
    extern const int NOT_ENOUGH_SPACE;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
//    extern const StorageActionBlockType PartsFetch;
//    extern const StorageActionBlockType PartsSend;
//    extern const StorageActionBlockType ReplicationQueue;
    extern const StorageActionBlockType PartsRecode;
    extern const StorageActionBlockType PartsBuildBitmap;
}

namespace
{
    ThrottlerPtr createThrottler(ContextPtr query_context)
    {
        if (size_t max_bandwidth = query_context->getSettingsRef().max_replicated_fetches_network_bandwidth_for_server)
            return std::make_shared<Throttler>(max_bandwidth, query_context->getReplicatedFetchesThrottler());
        else
            return query_context->getReplicatedFetchesThrottler();
    }
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

static std::string normalizeZooKeeperPath(std::string zookeeper_path)
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;

    return zookeeper_path;
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

using TableMetadata = ReplicatedMergeTreeTableMetadata;

StorageHaUniqueMergeTree::StorageHaUniqueMergeTree(
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
    bool has_force_restore_data_flag)
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
        [/*this*/](const std::string & /*name*/) { /*enqueuePartForCheck(name);*/ })
    , zookeeper_name(extractZooKeeperName(zookeeper_path_))
    , zookeeper_path(extractZooKeeperPath(zookeeper_path_))
    , replica_name(replica_name_)
    , replica_path(zookeeper_path + "/replicas/" + replica_name_)
    , reader(*this)
    , writer(*this)
    , merger_mutator(*this, getContext()->getSettingsRef().background_pool_size)
    , log_exchanger(*this)
    , fetcher(*this)
    , alter_thread(*this)
    , cleanup_thread(*this)
    , restarting_thread(*this)
{
    auto db_table = getStorageID().database_name + "." + getStorageID().table_name;
    repair_data_task = getContext()->getUniqueTableSchedulePool().createTask(db_table + " (RepairData)", [this] { return repairDataTask(); });
    become_leader_task = getContext()->getUniqueTableSchedulePool().createTask(db_table + " (BecomeLeader)", [this] { return becomeLeaderTask(); });
    update_log_task = getContext()->getUniqueTableSchedulePool().createTask(db_table + " (UpdateLog)", [this] { updateLogTask(); });
    replay_log_task = getContext()->getUniqueTableSchedulePool().createTask(db_table + " (ReplayLog)", [this] { return replayLogTask(); });
    checkpoint_log_task = getContext()->getUniqueTableSchedulePool().createTask(db_table + " (CheckpointLog)", [this] { checkpointLogTask(); });
    /// FIXME (UNIQUE KEY): Use BackgroundJobsExecutor
    merge_task_handle = getContext()->getUniqueTableSchedulePool().createTask(db_table + " (HaUniqueMergeTreeMergeTask)", [this] { return mergeTask(); });

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

        LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag {}/flags/force_restore_data).", replica_path);
    }
    else if (has_force_restore_data_flag)
    {
        skip_sanity_checks = true;

        LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag force_restore_data).");
    }

    loadDataParts(skip_sanity_checks, attach);

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
        LOG_WARNING(log, "No metadata in ZooKeeper for {}: table will be in readonly mode.", zookeeper_path);
        is_readonly = true;
        return;
    }

    auto metadata_snapshot = getInMemoryMetadataPtr();
    if (!metadata_snapshot->hasUniqueKey())
        throw Exception("Should have unique key specified...", ErrorCodes::LOGICAL_ERROR);

    /// May it be ZK lost not the whole root, so the upper check passed, but only the /replicas/replica
    /// folder.
    if (attach && !current_zookeeper->exists(replica_path))
    {
        LOG_WARNING(log, "No metadata in ZooKeeper for {}: table will be in readonly mode", replica_path);
        is_readonly = true;
        has_metadata_in_zookeeper = false;
        return;
    }

    // run_once_async_task = getContext()->getSchedulePool().createTask(log->name(), [this](){ runOnceAsyncTasks(); });
    // run_once_async_task->activate();
    // run_once_async_task->scheduleAfter(30000);

    if (!attach)
    {
        if (!getDataParts().empty())
            throw Exception(
                "Data directory for table already contains data parts"
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
                other_replicas_fixed_granularity = checkFixedGranularityInZookeeper();

                // checkTableStructure(zookeeper_path, metadata_snapshot);

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
        const bool replica_metadata_exists = current_zookeeper->tryGet(replica_path + "/metadata", replica_metadata);

        if (!replica_metadata_exists || replica_metadata.empty())
        {
            /// We have to check shared node granularity before we create ours.
            other_replicas_fixed_granularity = checkFixedGranularityInZookeeper();

            ReplicatedMergeTreeTableMetadata current_metadata(*this, metadata_snapshot);

            current_zookeeper->createOrUpdate(replica_path + "/metadata", current_metadata.toString(), zkutil::CreateMode::Persistent);
        }

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
        /// Temporary directories contain uninitialized results of Merges or Fetches (after forced restart),
        /// don't allow to reinitialize them, delete each of them immediately.
        clearOldTemporaryDirectories(0);
    }

    increment.set(getMaxBlockNumber());
}

bool StorageHaUniqueMergeTree::checkFixedGranularityInZookeeper()
{
    auto zookeeper = getZooKeeper();
    String metadata_str = zookeeper->get(zookeeper_path + "/metadata");
    auto metadata_from_zk = ReplicatedMergeTreeTableMetadata::parse(metadata_str);
    return metadata_from_zk.index_granularity_bytes == 0;
}

StorageHaUniqueMergeTree::~StorageHaUniqueMergeTree()
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

void StorageHaUniqueMergeTree::startup()
{
    LOG_DEBUG(log, "starting up table.");
    Stopwatch stopwatch;

    if (is_readonly)
        return;

    verifyTableStructureAlterIfNeed();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    /// NOTE: we update the part schema to the storage schema.
    alter_thread.part_metadata = TableMetadata(*this, metadata_snapshot).toString();
    alter_thread.part_columns = metadata_snapshot->getColumns().toString();

    replica_endpoint_holder = std::make_unique<HaReplicaEndpointHolder>(
        replica_path, std::make_shared<HaUniqueMergeTreeReplicaEndpoint>(*this), getContext()->getHaReplicaHandler());

    InterserverIOEndpointPtr data_parts_exchange_ptr = std::make_shared<DataPartsExchange::Service>(*this, shared_from_this());
    [[maybe_unused]] auto prev_ptr = std::atomic_exchange(&data_parts_exchange_endpoint, data_parts_exchange_ptr);
    assert(prev_ptr == nullptr);
    getContext()->getInterserverIOHandler().addEndpoint(data_parts_exchange_ptr->getId(replica_path), data_parts_exchange_ptr);

    // addRecodeAndBitMapTaskIfNeeded(true);

    restarting_thread.start();
    startup_event.wait();

    // collect_map_key_task = getContext()->getLocalSchedulePool().createTask(log->name() + " (MapKeyCache)", [this]() { runCollectMapKeyTasks(); });
    // collect_map_key_task->activateAndSchedule();

    LOG_DEBUG(log, "startup finished; cost {} ms.", stopwatch.elapsedMicroseconds() / 1000.0);
}


bool StorageHaUniqueMergeTree::verifyTableStructureAlterIfNeed(const String& metadata_str, const String& columns_str)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    TableMetadata local_metadata(*this, metadata_snapshot);

    auto metadata_from_manifest = TableMetadata::parse(metadata_str);
    auto columns_from_manifest = ColumnsDescription::parse(columns_str);

    auto metadata_diff = local_metadata.checkAndFindDiff(metadata_from_manifest);

    const ColumnsDescription & local_columns = metadata_snapshot->getColumns();
    if (columns_from_manifest != local_columns || !metadata_diff.empty())
    {
        LOG_INFO(log, "Table schema is different, starting to alter to schema: {}\n{}", metadata_str, columns_str);


        auto alter_lock = lockForAlter(RWLockImpl::NO_QUERY, getContext()->getSettingsRef().lock_acquire_timeout);
        alterTableSchema(metadata_str, columns_str, alter_lock);
        return true;
    }
    return false;
}

bool StorageHaUniqueMergeTree::verifyTableStructureAlterIfNeed()
{

    String metadata_str = manifest_store->getLatestTableMetadata();
    String columns_str = manifest_store->getLatestTableColumns();

    LOG_INFO(log, "Verifying table structure with latest manifest store schema: {}\n{}", metadata_str, columns_str);

    return verifyTableStructureAlterIfNeed(metadata_str, columns_str);
}

bool StorageHaUniqueMergeTree::verifyTableStructureForVersionAlterIfNeed(UInt64 version)
{
    LOG_INFO(log, "Verifying table structure at manifest version: {}", version);

    auto snapshot = manifest_store->getSnapshot(version);
    return verifyTableStructureAlterIfNeed(snapshot.metadata_str, snapshot.columns_str);
}

void StorageHaUniqueMergeTree::shutdown()
{
    fetcher.blocker.cancelForever();
    merger_mutator.merges_blocker.cancelForever();

    /// Stop recode task
    // stopRecodeForever();
    // if (recode_part_task)
    //     getContext()->getAdditionalBackgroundPool().removeTask(recode_part_task);

    // stopBuildBitMapForever();
    // if (build_bitmap_task)
    //     getContext()->getAdditionalBackgroundPool().removeTask(build_bitmap_task);

    // if (modify_ttl_task)
    //     getContext()->getAdditionalBackgroundPool().removeTask(modify_ttl_task);

    repair_data_task->deactivate();
    restarting_thread.shutdown();

    if (replica_endpoint_holder)
        replica_endpoint_holder = nullptr;

    auto data_parts_exchange_ptr = std::atomic_exchange(&data_parts_exchange_endpoint, InterserverIOEndpointPtr{});
    if (data_parts_exchange_ptr)
    {
        getContext()->getInterserverIOHandler().removeEndpointIfExists(data_parts_exchange_ptr->getId(replica_path));
        /// Ask all parts exchange handlers to finish asap. New ones will fail to start
        data_parts_exchange_ptr->blocker.cancelForever();
        /// Wait for all of them
        std::unique_lock lock(data_parts_exchange_ptr->rwlock);
    }

    // if (collect_map_key_task.hasTask())
    //     collect_map_key_task->deactivate();
}

void StorageHaUniqueMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned int num_streams)
    // MergeTreeReadPlannerPtr read_planner,
    // MergeTreeBitMapSchedulerPtr bitmap_scheduler)
{
    if (auto plan = reader.read(column_names, metadata_snapshot, query_info, query_context, max_block_size, num_streams, processed_stage /*read_planner, bitmap_scheduler*/))
        query_plan = std::move(*plan);
}


Pipe StorageHaUniqueMergeTree::read(
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

void StorageHaUniqueMergeTree::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode (zookeeper path: {})", zookeeper_path);
}

BlockOutputStreamPtr StorageHaUniqueMergeTree::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    /// mainly used for tests and for create temporary table to delay inserts until leader is selected
    auto settings = getSettings();
    auto wait_timeout = std::max(
        query_context->getSettingsRef().max_insert_wait_seconds_for_unique_table_leader, settings->unique_engine_temp_table_wait_interval);
    if (wait_timeout > 0)
    {
        LOG_DEBUG(log, "Wait until leader election for {} seconds.", wait_timeout);
        auto zookeeper = getZooKeeper();
        Stopwatch timer;
        while (!is_leader)
        {
            String leader_replica_name = getCachedLeader();
            if (!leader_replica_name.empty() && leader_replica_name != replica_name)
                break; /// found a leader replica to forward request
            if (timer.elapsedSeconds() > wait_timeout)
                throw Exception(
                    "Can't write to " + getStorageID().database_name + "." + getStorageID().table_name
                        + " because there is no leader right now",
                    ErrorCodes::READONLY);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    /// TODO offline node handling
    return std::make_shared<HaUniqueMergeTreeBlockOutputStream>(
        *this, metadata_snapshot, query_context, query_context->getSettingsRef().max_partitions_per_insert_block);
}

void StorageHaUniqueMergeTree::drop()
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
            throw Exception(
                "Can't drop readonly replicated table (need to drop data in ZooKeeper as well)", ErrorCodes::TABLE_IS_READ_ONLY);

        shutdown();
        dropReplica(zookeeper, zookeeper_path, replica_name, log);
    }

    dropAllData();
}

void StorageHaUniqueMergeTree::dropReplica(
    zkutil::ZooKeeperPtr zookeeper, const String & zookeeper_path, const String & replica, Poco::Logger * logger)
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
        LOG_ERROR(
            logger,
            "Replica was not completely removed from ZooKeeper, {} still exists and may contain some garbage.",
            remote_replica_path);

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

bool StorageHaUniqueMergeTree::removeTableNodesFromZooKeeper(
    zkutil::ZooKeeperPtr zookeeper,
    const String & zookeeper_path,
    const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock,
    Poco::Logger * logger)
{
    bool completely_removed = false;
    Strings children;
    Coordination::Error code = zookeeper->tryGetChildren(zookeeper_path, children);
    if (code == Coordination::Error::ZNONODE)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of replicated table. It's a bug");


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
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of replicated table. It's a bug");
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_ERROR(
            logger,
            "Table was not completely removed from ZooKeeper, {} still exists and may contain some garbage,"
            "but someone is removing it right now.",
            zookeeper_path);
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

void StorageHaUniqueMergeTree::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
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
            zookeeper->set(fs::path(replica_path) / "host", getHaUniqueMergeTreeAddress().toString());
        }
        catch (Coordination::Exception & e)
        {
            LOG_WARNING(log, "Cannot update the value of 'host' node (replica address) in ZooKeeper: {}", e.displayText());
        }
    }

    /// TODO: You can update names of loggers.
}

ActionLock StorageHaUniqueMergeTree::getActionLock(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return merger_mutator.merges_blocker.cancel();

    // if (action_type == ActionLocks::PartsRecode)
    //     return stopRecode();

    // if (action_type == ActionLocks::PartsBuildBitmap)
    //     return stopBuildBitMap();

    return {};
}

void StorageHaUniqueMergeTree::setZooKeeper()
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

zkutil::ZooKeeperPtr StorageHaUniqueMergeTree::tryGetZooKeeper() const
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr StorageHaUniqueMergeTree::getZooKeeper() const
{
    auto res = tryGetZooKeeper();
    if (!res)
        throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);
    return res;
}

bool StorageHaUniqueMergeTree::createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot)
{
    Stopwatch stopwatch;
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
        String metadata_str = TableMetadata(*this, metadata_snapshot).toString();

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", metadata_str, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            zookeeper_path + "/columns", metadata_snapshot->getColumns().toString(), zkutil::CreateMode::Persistent));
        /// We put zero LSN for easier handling
        ops.emplace_back(zkutil::makeCreateRequest(getZKLatestLSNPath(), "0", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(getZKLatestLSNPath() + "/lsn-", "", zkutil::CreateMode::EphemeralSequential));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/block_numbers", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/leader_election", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/temp", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(
            zkutil::makeCreateRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name, zkutil::CreateMode::Persistent));

        /// And create first replica atomically. See also "createReplica" method that is used to create not the first replicas.

        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/committed_lsn", "0", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/flags", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/is_lost", "0", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/columns", metadata_snapshot->getColumns().toString(), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata", String(), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/metadata_version", std::to_string(metadata_version), zkutil::CreateMode::Persistent));


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
    throw Exception(
        "Cannot create table, because it is created concurrently every time "
        "or because of wrong zookeeper_path "
        "or because of logical error",
        ErrorCodes::REPLICA_IS_ALREADY_EXIST);
}


void StorageHaUniqueMergeTree::createReplica(const StorageMetadataPtr & metadata_snapshot)
{
    Stopwatch stopwatch;
    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Creating replica {}", replica_path);

    Coordination::Error code;
    do
    {
        Coordination::Stat replicas_stat;
        String replicas_value;

        if (!zookeeper->tryGet(zookeeper_path + "/replicas", replicas_value, &replicas_stat))
            throw Exception(
                ErrorCodes::ALL_REPLICAS_LOST,
                "Cannot create a replica of the table {}, because the last replica of the table was dropped right now",
                zookeeper_path);

        /// It is not the first replica, we will mark it as "lost", to immediately repair (clone) from existing replica.
        /// By the way, it's possible that the replica will be first, if all previous replicas were removed concurrently.
        const String is_lost_value = replicas_stat.numChildren ? "1" : "0";

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/committed_lsn", "0", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/flags", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/is_lost", is_lost_value, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/columns", metadata_snapshot->getColumns().toString(), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata", String(), zkutil::CreateMode::Persistent));
        /// Check version of /replicas to see if there are any replicas created at the same moment of time.
        ops.emplace_back( zkutil::makeSetRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name, replicas_stat.version));
        ops.emplace_back(zkutil::makeCreateRequest(
            replica_path + "/metadata_version", std::to_string(metadata_version), zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        code = zookeeper->tryMulti(ops, responses);

        switch (code)
        {
            case Coordination::Error::ZNODEEXISTS:
                throw Exception(ErrorCodes::REPLICA_IS_ALREADY_EXIST, "Replica {} already exists", replica_path);
            case Coordination::Error::ZBADVERSION:
                LOG_ERROR(log, "Retrying createReplica(), because some other replicas were created at the same time");
                break;
            case Coordination::Error::ZNONODE:
                throw Exception(ErrorCodes::ALL_REPLICAS_LOST, "Table {} was suddenly removed", zookeeper_path);
            default:
                zkutil::KeeperMultiException::check(code, ops, responses);
        }
    } while (code == Coordination::Error::ZBADVERSION);

    LOG_DEBUG(log, "Created replica {}; cost {} ms", replica_path, stopwatch.elapsedMicroseconds() / 1000.0);
}

HaMergeTreeAddress StorageHaUniqueMergeTree::getHaUniqueMergeTreeAddress() const
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

time_t StorageHaUniqueMergeTree::getAbsoluteDelay([[maybe_unused]] const String & pred) const
{
    /// leader always has no delay
    if (is_leader)
        return 0;

    time_t current_time = time(nullptr);
    /// TODO return maximum delay for offline node

    /// return maximum delay for abnormal replica
    if (unique_table_state != UniqueTableState::NORMAL)
        return current_time;

    /// if there are uncommitted logs in queue, use time since last commit as delay
    if (manifest_store->latestVersion() > manifest_store->commitVersion())
        return current_time - manifest_store->timeOfLastCommitLog();

    /// if all local logs are committed and local logs are no more than 1 minute behind the leader,
    /// return no delay to allow follower node to share some query workload
    if (last_update_log_time.load(std::memory_order_relaxed) + 60 > current_time)
        return 0;
    /// if all local logs are committed but local logs may fall behind leader,
    /// use time since last commit as delay
    else
        return current_time - manifest_store->timeOfLastCommitLog();
}

void StorageHaUniqueMergeTree::checkSessionIsNotExpired(zkutil::ZooKeeperPtr & zookeeper)
{
    if (!zookeeper)
        throw Exception("No ZooKeeper session.", ErrorCodes::NO_ZOOKEEPER);

    if (zookeeper->expired())
        throw Exception("ZooKeeper session has been expired.", ErrorCodes::NO_ZOOKEEPER);
}

UInt64 StorageHaUniqueMergeTree::allocateBlockNumberDirect(zkutil::ZooKeeperPtr & zookeeper)
{
    String block_numbers_path = zookeeper_path + "/block_numbers";
    String block_numbers_path_prefix = block_numbers_path + "/block-";
    auto block_path = zookeeper->create(block_numbers_path_prefix, "", zkutil::CreateMode::EphemeralSequential);
    auto block_number = parse<UInt64>(block_path.substr(block_numbers_path_prefix.length()));
    return block_number;
}

std::pair<UInt64, Coordination::RequestPtr> StorageHaUniqueMergeTree::allocLSNAndMakeSetRequest(zkutil::ZooKeeperPtr & zookeeper)
{
    checkSessionIsNotExpired(zookeeper);
    auto node_prefix = getZKLatestLSNPath() + "/lsn-";
    auto created_path = zookeeper->create(node_prefix, "", zkutil::CreateMode::EphemeralSequential);
    auto num = parse<UInt64>(created_path.substr(node_prefix.length()));
    LOG_TRACE(log, "Allocate new LSN-{}, and make a request", num);
    return {num, zkutil::makeSetRequest(getZKLatestLSNPath(), toString(num), -1)};
}

UInt64 StorageHaUniqueMergeTree::allocLSNAndSet(zkutil::ZooKeeperPtr & zookeeper)
{
    checkSessionIsNotExpired(zookeeper);
    auto node_prefix = getZKLatestLSNPath() + "/lsn-";
    auto created_path = zookeeper->create(node_prefix, "", zkutil::CreateMode::EphemeralSequential);
    auto numString = created_path.substr(node_prefix.length());
    auto new_lsn = parse<UInt64>(numString);
    zookeeper->set(getZKLatestLSNPath(), toString(new_lsn));
    LOG_TRACE(log, "Allocate and set new LSN-{}", numString);
    return new_lsn;
}

bool StorageHaUniqueMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    auto lock = mergeSelectLock(/*acquire=*/true);
    return parts_under_merge.count(part) > 0;
}

void StorageHaUniqueMergeTree::enterElectionUnlocked(const ElectionLock &)
{
    Stopwatch stopwatch;

    auto callback = [this]()
    {
        std::unique_lock<std::mutex> term_lock(term_mutex);
        last_term_of_become_leader = ++election_term;
        /// reset to zero before becomeLeaderTask is scheduled for this attempt
        become_leader_start_time = 0;
        become_leader_at_version = 0;
        LOG_INFO(log, "Trying to become leader for term {}", last_term_of_become_leader);
        become_leader_task->activateAndSchedule();
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
            /*allow_multiple_leaders*/false);
    }
    catch (...)
    {
        leader_election = nullptr;
        throw;
    }

    LOG_DEBUG(log, "Created leader election; cost {} ms.", stopwatch.elapsedMicroseconds() / 1000.0);
}

void StorageHaUniqueMergeTree::exitElectionUnlocked(const ElectionLock &)
{
    if (!leader_election)
        return;

    /// Shut down the leader election thread to avoid suddenly becoming the leader again after
    /// we have stopped leader-only tasks, but before we have deleted the leader_election object.
    leader_election->shutdown();

    if (is_leader)
    {
        CurrentMetrics::sub(CurrentMetrics::LeaderReplica);
        LOG_INFO(log, "Stopped being leader");
        is_leader = false;
        /// stopping leader only tasks (which shouldn't use ElectionLock)
        checkpoint_log_task->deactivate();
        /// notify and wait for all running merge tasks to abort
        auto merge_block = merger_mutator.merges_blocker.cancel();
        auto ttl_block = merger_mutator.ttl_merges_blocker.cancel();
        // getContext()->getUniqueTableSchedulePool().removeTask(merge_task_handle);
        merge_task_handle->deactivate();
    }

    /// Delete the node in ZK only after we have stopped the merge_selecting_thread - so that only one
    /// replica assigns merges at any given time.
    leader_election = nullptr;

    std::unique_lock<std::mutex> term_lock(term_mutex);
    last_term_of_exit_leader = ++election_term;
    LOG_INFO(log, "Exit leader election at term {}", last_term_of_exit_leader);
}

/// A node becomes leader in two phases:
/// - phase 1: Acquire leadership on zk and become leader candidate
/// - phase 2: Wait for local commit version to catch up previous leader.
/// phase 2 could last for minutes, so we schedule the below task periodically to check for its termination.
void StorageHaUniqueMergeTree::becomeLeaderTask()
{
    auto lock = electionLock();

    time_t start_time;
    UInt64 target_version;
    /// It could take us minutes to become leader and during that period we could lost leadership.
    {
        std::unique_lock<std::mutex> term_lock(term_mutex);
        if (last_term_of_become_leader < last_term_of_exit_leader)
        {
            LOG_INFO(log, "Stop becoming leader (term:{}) because we have lost leadership at term {}", last_term_of_become_leader, last_term_of_exit_leader);
            return;
        }
        if (become_leader_start_time == 0)
        {
            become_leader_start_time = time(nullptr);
            become_leader_at_version = parse<UInt64>(getZooKeeper()->get(getZKLatestLSNPath()));
        }
        start_time = become_leader_start_time;
        target_version = become_leader_at_version;
    }

    auto become_leader_action = [&](const String & reason, bool update = true) -> void
    {
        if (update)
            getZooKeeper()->set(getZKLeaderPath(), replica_name);
        LOG_INFO(log, "Became leader for term {} : {}", last_term_of_become_leader, reason);
        is_leader = true;
        CurrentMetrics::add(CurrentMetrics::LeaderReplica);
        /// stopping non-leader tasks
        update_log_task->deactivate();
        replay_log_task->deactivate();
        /// starting leader-only tasks
        checkpoint_log_task->activateAndSchedule();
        merge_task_handle->activateAndSchedule();
    };

    try
    {
        String last_leader = getZooKeeper()->get(getZKLeaderPath());
        /// case: the first leader
        if (last_leader.empty())
            return become_leader_action("first leader");

        /// case: replicas yield leadership to the last leader
        /// TODO: conform that replace node won't reuse replica name
        if (last_leader == replica_name)
            return become_leader_action("leader not change", false);

        /// case: current replica has complete data as the last leader
        if (manifest_store->commitVersion() == target_version)
            return become_leader_action("catching up latest version " + toString(target_version));

        auto peer_stats = log_exchanger.getAllManifestStatus();
        for (auto & peer_stat : peer_stats)
        {
            if (peer_stat.second.commit_version > manifest_store->commitVersion())
            {
                /// if the old-leader becomes active again, instead of waiting for replication
                /// we could immediately yield leadership for faster recovery
                LOG_INFO(log, "Found replica {} to be more up-to-date, yield leadership", peer_stat.first);
                exitElectionUnlocked(lock);
                enterElectionUnlocked(lock);
                return;
            }
            /// case: last leader restarts and we contains the same data as last leader
            if (peer_stat.first == last_leader && peer_stat.second.commit_version == manifest_store->commitVersion())
                return become_leader_action("catching-up old leader's version " + toString(peer_stat.second.commit_version));
        }

        // If we can't replay to the latest version for a long time (in case the old leader has crashed and some data was lost forever),
        // we should force become leader in order to serve new requests
        auto settings = getSettings();
        auto metadata_snapshot = getInMemoryMetadataPtr();
        if (start_time + settings->ha_unique_become_leader_timeout < static_cast<UInt64>(time(nullptr)))
        {
            /// before resetting manifest, stop tasks that may modify manifest concurrently
            update_log_task->deactivate();
            replay_log_task->deactivate();

            auto manifest_lock = manifest_store->writeLock();
            auto zookeeper = getZooKeeper();
            UInt64 new_version = allocLSNAndSet(zookeeper);
            UInt64 reset_version = manifest_store->commitVersion();
            manifest_store->discardFrom(manifest_lock, reset_version + 1);

            bool schema_changed = false;
            String old_columns = metadata_snapshot->getColumns().toString();
            String old_metadata = TableMetadata(*this, metadata_snapshot).toString();
            try
            {
                schema_changed = verifyTableStructureForVersionAlterIfNeed(reset_version);

                ManifestLogEntry entry;
                entry.type = ManifestLogEntry::RESET_MANIFEST;
                entry.version = new_version;
                entry.prev_version = reset_version;
                entry.source_replica = replica_name;
                entry.target_version = reset_version;
                manifest_store->append(manifest_lock, entry, true);

                if (schema_changed)
                    alter_thread.wakeup();
            }
            catch (...)
            {
                if (schema_changed)
                {
                    verifyTableStructureAlterIfNeed(old_metadata, old_columns);
                    alter_thread.wakeup();
                }
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                throw;
            }

            return become_leader_action("force become leader and reset manifest to " + toString(reset_version));
        }
    }
    catch (...)
    {
        /// just retry when any error happens.
        /// if ZK session expires, eventually restarting thread would call exitLeaderElection and
        /// we'll stop becoming leader on the next retry
        tryLogCurrentException(log);
    }
    /// wait for local commit version to catch-up latest version
    become_leader_task->scheduleAfter(10 * 100);
}

HaMergeTreeAddress StorageHaUniqueMergeTree::getReplicaAddress(const String & replica_name_)
{
    String replica_host_path = zookeeper_path + "/replicas/" + replica_name_ + "/host";
    return HaMergeTreeAddress(getZooKeeper()->get(replica_host_path));
}

String StorageHaUniqueMergeTree::refreshLeaderIfNeeded(const zkutil::ZooKeeperPtr & zookeeper)
{
    std::unique_lock<std::mutex> lock(refresh_leader_mutex);
    time_t now = time(nullptr);
    auto settings = getSettings();
    if (leader_replica_last_refresh_time + settings->ha_unique_refresh_leader_interval < static_cast<UInt64>(now))
    {
        leader_replica_last_refresh_time = now;
        String new_leader = zookeeper->get(getZKLeaderPath());
        if (!new_leader.empty())
            leader_address = getReplicaAddress(new_leader);
        if (new_leader != leader_replica)
        {
            leader_replica = new_leader;
            LOG_INFO(log, "Detect new leader to be {}", leader_replica);
        }
    }
    return leader_replica;
}

String StorageHaUniqueMergeTree::getCachedLeader(HaMergeTreeAddress * addr) const
{
    std::unique_lock<std::mutex> lock(refresh_leader_mutex);
    if (addr)
        *addr = leader_address;
    return leader_replica;
}

void StorageHaUniqueMergeTree::saveCommitVersionToZkIfNeeded(const zkutil::ZooKeeperPtr & zookeeper)
{
    UInt64 curr_commit_version = manifest_store->commitVersion();
    if (last_save_commit_version == curr_commit_version)
        return;
    time_t now = time(nullptr);
    if (last_save_commit_version_time + getSettings()->ha_unique_save_commit_version_interval < static_cast<UInt64>(now))
    {
        zookeeper->set(replica_path + "/committed_lsn", toString(curr_commit_version));
        LOG_TRACE(log, "Updated zk commit version from {} to {}", last_save_commit_version, curr_commit_version);
        last_save_commit_version = curr_commit_version;
        last_save_commit_version_time = now;
    }
}

void StorageHaUniqueMergeTree::refreshClusterCommitVersion(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// periodically persist current replica's commit version to ZK
    saveCommitVersionToZkIfNeeded(zookeeper);

    std::ostringstream detail_ss;
    UInt64 min_commit_version = manifest_store->commitVersion();
    detail_ss << replica_name << "=" << min_commit_version << " ";

    /// for active replicas, get commit version directly
    auto activeReplicaInfos = log_exchanger.getAllManifestStatus();
    for (auto & entry : activeReplicaInfos)
    {
        min_commit_version = std::min(min_commit_version, entry.second.commit_version);
        detail_ss << entry.first << "=" << entry.second.commit_version << " ";
    }

    /// for non-active replicas
    /// - if the replica is not lost, get cached commit version from ZK
    /// - if the replica is under repair, get repair version from ZK
    /// - if the replica is lost, ignore (prevent lost replica from freezing cluster commit version)
    auto replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
    for (auto & replica : replicas)
    {
        if (replica == replica_name || activeReplicaInfos.count(replica))
            continue;
        String data;
        UInt64 commit_version = 0;
        auto base_path = zookeeper_path + "/replicas/" + replica;
        if (zookeeper->get(base_path + "/is_lost") == "0")
        {
            commit_version = parse<UInt64>(zookeeper->get(base_path + "/committed_lsn"));
            detail_ss << replica << "=" << commit_version << "(zk) ";
        }
        else if (zookeeper->tryGet(base_path + "/repair_lsn", data))
        {
            commit_version = parse<UInt64>(data);
            detail_ss << replica << "=" << commit_version << "(repair) ";
        }
        else
        {
            detail_ss << replica << "(lost) ";
            continue;
        }
        min_commit_version = std::min(min_commit_version, commit_version);
    }
    unique_commit_version.store(min_commit_version, std::memory_order_release);
    LOG_TRACE(log, "Update cluster commit version to {}, detail: {}", min_commit_version, detail_ss.str());
}

/// only for non-leader
void StorageHaUniqueMergeTree::updateLogTask()
{
    bool success = true;
    auto settings = getSettings();
    SCOPE_EXIT({
        if (success)
            last_update_log_time.store(time(nullptr), std::memory_order_relaxed);
        update_log_task->scheduleAfter(settings->ha_unique_update_log_sleep_ms);
    });

    if (/*!getContext()->isListenPortsReady() || */ is_leader)
    {
        success = false;
        return;
    }

    try
    {
        auto zookeeper = getZooKeeper();
        auto cached_leader = refreshLeaderIfNeeded(zookeeper);
        if (cached_leader.empty())
            return;

        /// get latest lsn from zk
        auto zk_latest_lsn = parse<UInt64>(zookeeper->get(getZKLatestLSNPath()));
        if (zk_latest_lsn == manifest_store->latestVersion())
            return;

        /// fetch more logs from leader
        auto fetch_from_version = manifest_store->latestVersion() + 1;
        auto entries = log_exchanger.getLogEntries(cached_leader, fetch_from_version, /*limit=*/100);
        if (entries.empty())
            return;

        ManifestLogEntry::checkConsecutiveLogs(entries);

        auto & first_entry = entries.front();
        /// Only RESET_MANIFEST can discard logs. So if the first entry is not RESET_MANIFEST,
        /// its prev version must match last log's version
        if (first_entry.type != ManifestLogEntry::RESET_MANIFEST)
        {
            if (first_entry.prev_version != manifest_store->latestVersion())
                throw Exception(
                    "Fetching logs from " + cached_leader + " at version " + toString(fetch_from_version)
                        + " but got non-consecutive log:\n" + first_entry.toText(),
                    ErrorCodes::LOGICAL_ERROR);
        }
        else
        {
            auto reset_version = first_entry.target_version;
            /// Local manifest could be in on of the following states
            /// case 1: has log at reset_version, no logs > reset_version
            /// case 2: has log at reset_version, has logs > reset_version, but none of them are committed
            /// case 3: has log at reset_version, has logs > reset_version, and some of them are committed
            /// case 4: missing log at reset_version, has logs > reset_version but none are committed
            /// case 5: missing log at reset_version, has logs > reset_version and some are committed
            /// Example
            ///          |-------- local logs -------|--- fetched logs ---|- expected logs after update -|
            /// case 1 : |       9(commit) 10        | 13(reset to 10)    |    9(commit) 10 13(false)    |
            /// case 2 : |     9 10(commit) 11 12    | 13(reset to 10)    |    9 10(commit) 13(false)    |
            /// case 3 : |     9 10 11(commit) 12    | 13(reset to 10)    |    9 10(commit) 13(true)     |
            /// case 4 : |       9(commit) 11        | 13(reset to 10)    |    9(commit) 10 13(false)    |
            /// case 5 : |       9 11(commit)        | 13(reset to 10)    |    9(commit) 10 13(true)     |
            ///
            /// In all cases, we should remove all logs > reset_version. If any of the them are committed before,
            /// we should mark the RESET_MANIFEST log for later execution and update commit version.
            /// For case 4 and 5, we should retry log fetching so that we can fetch the missing log in the next run.

            std::unique_lock<std::mutex> replay_log_lock(replay_log_mutex);
            auto manifest_lock = manifest_store->writeLock();
            /// case 3 and 5
            if (manifest_store->commitVersion() > reset_version)
            {
                execute_reset_manifest[first_entry.version] = true;
            }
            if (manifest_store->latestVersion() > reset_version)
            {
                manifest_store->discardFrom(manifest_lock, reset_version + 1);
            }
            /// case 4 and 5
            if (!manifest_store->containsLog(reset_version))
                return;
        }

        auto manifest_lock = manifest_store->writeLock();
        manifest_store->append(manifest_lock, entries, /*commit=*/false);
        LOG_TRACE(log, "Added {} logs to manifest", entries.size());
        replay_log_task->schedule();
    }
    catch (const Exception & e)
    {
        success = false;
        if (e.code() == ErrorCodes::NO_ACTIVE_REPLICAS)
            LOG_WARNING(log, "Failed to update manifest logs, {}", getExceptionMessage(e, false));
        else
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
    catch (...)
    {
        success = false;
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

/// only for non-leader
/// each run only replays one log so that all tasks have change to run and
/// we can respond to deactivate more quickly
void StorageHaUniqueMergeTree::replayLogTask()
{
    std::unique_lock<std::mutex> replay_log_lock(replay_log_mutex);
    auto settings = getSettings();
    SCOPE_EXIT({
        if (last_executing_log.num_failed > 0)
            replay_log_task->scheduleAfter(last_executing_log.retry_backoff_ms);
        else if (manifest_store->commitVersion() == manifest_store->latestVersion())
            replay_log_task->scheduleAfter(settings->ha_unique_replay_log_sleep_ms);
        else
            replay_log_task->schedule();
    });

    if (/*!getContext()->isListenPortsReady() || */ is_leader)
        return;

    try
    {
        if (settings->ha_unique_replay_log_add_delay)
        {
            // Only test purpose
            std::this_thread::sleep_for(std::chrono::milliseconds(settings->ha_unique_replay_log_add_delay));
        }
        /// get the next uncommitted log
        auto entries = manifest_store->getLogEntries(manifest_store->commitVersion() + 1, 1);
        if (entries.empty())
            return;
        auto & entry = entries.front();
        if (entry.version != last_executing_log.version)
            last_executing_log = {entry.version, 0, settings->ha_unique_replay_log_retry_backoff_min_ms};

        bool schema_changed = false;
        String old_columns = getInMemoryMetadataPtr()->getColumns().toString();
        String old_metadata = TableMetadata(*this, getInMemoryMetadataPtr()).toString();
        try
        {
            switch (entry.type)
            {
                case ManifestLogEntry::SYNC_PARTS:
                    executeSyncParts(entry);
                    break;
                case ManifestLogEntry::DETACH_PARTS:
                    executeDeatchParts(entry);
                    break;
                case ManifestLogEntry::RESET_MANIFEST: {
                    executeResetManifest(entry);
                    schema_changed = verifyTableStructureForVersionAlterIfNeed(entry.version);
                    break;
                }
                case ManifestLogEntry::CHECKPOINT_MANIFEST:
                    executeCheckpointManifest(entry);
                    break;
                case ManifestLogEntry::MODIFY_SCHEMA: {
                    executeModifySchemaManifest(entry);
                    schema_changed = true;
                    break;
                }
                default:
                    throw Exception("Unknown manifest log type: " + toString<int>(entry.type), ErrorCodes::LOGICAL_ERROR);
            }
            auto lock = manifest_store->writeLock();
            manifest_store->commitLog(lock, entry.version);

            if (schema_changed)
                alter_thread.wakeup();
        }
        catch (...)
        {
            if (schema_changed)
            {
                verifyTableStructureAlterIfNeed(old_metadata, old_columns);
                alter_thread.wakeup();
            }
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            throw;
        }
    }
    catch (...)
    {
        last_executing_log.num_failed++;
        last_executing_log.retry_backoff_ms
            = std::min(last_executing_log.retry_backoff_ms * 2, settings->ha_unique_replay_log_retry_backoff_max_ms.value);
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void StorageHaUniqueMergeTree::executeModifySchemaManifest(const ManifestLogEntry & entry)
{
    LOG_DEBUG(log, "Replica executing alter replay.");
    try
    {

        auto alter_lock = lockForAlter(RWLockImpl::NO_QUERY, getContext()->getSettingsRef().lock_acquire_timeout);
        alterTableSchema(entry.metadata_str, entry.columns_str, alter_lock);

        alter_thread.wakeup();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        throw;
    }
}

void StorageHaUniqueMergeTree::alterTableSchema(const String& new_metadata_str, const String& new_columns_str, TableLockHolder & /*lock*/)
{

    auto columns_from_entry = ColumnsDescription::parse(new_columns_str);
    auto metadata_from_entry = ReplicatedMergeTreeTableMetadata::parse(new_metadata_str);

    LOG_INFO(log, "Metadata changed from leader side. Applying changes locally.");

    auto metadata_diff = ReplicatedMergeTreeTableMetadata(*this, getInMemoryMetadataPtr()).checkAndFindDiff(metadata_from_entry);
    setTableStructure(std::move(columns_from_entry), metadata_diff);

    LOG_INFO(log, "Applied changes to the metadata of the table. Current metadata version: {}", metadata_version);
}

void StorageHaUniqueMergeTree::executeSyncParts(const ManifestLogEntry & entry)
{
    if (!entry.added_parts.empty() || !entry.updated_parts.empty())
    {
        /// If added_parts is empty, it means that the corresponding insert operation just deletes some rows with the help of _delete_flag_ column.
        String replica = chooseReplicaToFetch(entry.version);
        fetchAndLoadData(
            replica,
            entry.added_parts.empty() ? "" : entry.added_parts.front(),
            entry.updated_parts,
            entry.version,
            createThrottler(getContext()));
    }
    /// for drop partition that only deletes parts
    else
    {
        DataPartsVector to_remove;
        for (auto & part_name : entry.removed_parts)
        {
            auto part = getPartIfExists(part_name, {DataPartState::Committed});
            if (part == nullptr)
                LOG_WARNING(log, "Can't find the part {} to remove for version {}", part_name, entry.version);
            else
            {
                to_remove.emplace_back(part);
            }
        }
        removePartsFromWorkingSet(to_remove, /*clear_without_timeout=*/true);
    }
}

void StorageHaUniqueMergeTree::executeDeatchParts(const ManifestLogEntry & entry)
{
    LOG_INFO(log, "Executing deatching parts size: {}", entry.removed_parts.size());
    DataPartsVector to_remove;
    for (auto & part_name : entry.removed_parts)
    {
        auto part = getPartIfExists(part_name, {DataPartState::Committed});
        if (part == nullptr)
        {
            LOG_WARNING(log, "Can't find the part {} to remove for version {}", part_name, entry.version);
        }
        else
        {
            const_cast<IMergeTreeDataPart &>(*part).loadChecksumsIfNeed();
            to_remove.emplace_back(part);
        }
    }
    removePartsFromWorkingSet(to_remove, /*clear_without_timeout=*/true);
    for (const auto & part : to_remove)
    {
        LOG_INFO(log, "Replay detaching {}", part->relative_path);
        part->makeCloneInDetached("", getInMemoryMetadataPtr());
    }
}

String StorageHaUniqueMergeTree::chooseReplicaToFetch(UInt64 version)
{
    /// among all replicas that has the data, choose the one with lowest load
    auto replica_statuses = log_exchanger.getAllManifestStatus();
    String replica;
    UInt64 min_load = 0;
    for (auto & [replica_name, status] : replica_statuses)
    {
        if (status.commit_version < version)
            continue;
        if (replica.empty() || status.num_running_sends < min_load)
        {
            replica = replica_name;
            min_load = status.num_running_sends;
        }
    }
    if (replica.empty())
        throw Exception("No active replica contains the data for log version " + toString(version), ErrorCodes::NO_REPLICA_HAS_PART);
    return replica;
}

void StorageHaUniqueMergeTree::fetchAndLoadData(
    const String & replica,
    const String & part_name,
    const Strings & updated_part_names,
    UInt64 delete_version,
    const ThrottlerPtr & throttler)
{
    if (part_name.empty() && updated_part_names.empty())
        throw Exception(
            "No new part is generated and no part is updated for log version " + toString(delete_version), ErrorCodes::LOGICAL_ERROR);
    if (!part_name.empty())
    {
        auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

        /// When fetching merged part, we may contain an outdated part with the same name in the following case:
        /// T1: we have the following logs and parts
        ///     versions:        10                     11                     12
        ///        parts:  part_1_1_0(outdated)   part_2_2_0(outdated)   part_1_2_1(committed)
        /// T2: new leader resets manifest to version 11, therefore our local state is changed to
        ///     versions:        10                     11                     12(discarded)         13(reset to 11)
        ///        parts:  part_1_1_0(committed)   part_2_2_0(committed)   part_1_2_1(outdated)
        /// T3: leader merges part 1 and 2 and creates part_1_2_1 again. Now fetching will fail because part_1_2_1 exists locally.
        ///
        /// In this case, we must remove the local part_1_2_1 and re-fetch it from the leader
        /// because the two parts despite having the same name, could contain different data.
        /// For example, leader could read version 14 of part 1 and 2 to create part_1_2_1 while our outdated part is based on version 11.
        ///     versions:      10            11      13(reset to 11)    14              15
        ///        parts:  part_1_1_0   part_2_2_0                   part_3_3_0    part_1_2_1
        ///
        /// Non-merged part doesn't have this issue because its name is based on block number which is always increasing.
        if (part_info.level > 0)
        {
            auto lock = lockParts();
            auto it = data_parts_by_info.find(part_info);
            if (it != data_parts_by_info.end())
            {
                auto name_with_state = (*it)->getNameWithState();
                if ((*it)->getState() == DataPartState::Committed)
                    throw Exception("Trying to fetch a committed part " + name_with_state, ErrorCodes::LOGICAL_ERROR);
                if (DataPartPtr part = *it)
                {
                    modifyPartState(it, DataPartState::Deleting);
                    manifest_store->deletePartRemovedVersion({part_name});
                    part->remove();
                    removePartsFinallyUnsafe({part}, &lock);
                    LOG_DEBUG(log, "Removed part {} before fetch", name_with_state);
                }
            }
        }
    }

    UInt64 version = delete_version;
    if (!part_name.empty())
        LOG_DEBUG(log, "Fetching part {} and {} delete files from {}", part_name, updated_part_names.size(), replica);
    else
        LOG_DEBUG(log, "Fetching {} delete files from {}", updated_part_names.size(), replica);

    DataParts updated_parts;
    for (auto & name : updated_part_names)
    {
        auto part = getPartIfExists(name, {DataPartState::Committed});
        if (part == nullptr)
            throw Exception("Trying to update part " + name + " which doesn't exist", ErrorCodes::NO_SUCH_DATA_PART);
        updated_parts.emplace(std::move(part));
    }

    auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

    MergeTreeData::MutableDataPartPtr part;
    MergeTreeData::DataPartsVector replace_parts;
    Stopwatch stopwatch;

    auto write_part_log = [&](const ExecutionStatus & status) {
        if (!part_name.empty())
            writePartLog(PartLogElement::DOWNLOAD_PART, status, stopwatch.elapsed(), part_name, part, replace_parts, nullptr);
    };

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(getContext());
    auto credentials = getContext()->getInterserverCredentials();
    String interserver_scheme = getContext()->getInterserverScheme();
    String zk_replica_path = zookeeper_path + "/replicas/" + replica;
    try
    {
        HaMergeTreeAddress address(getZooKeeper()->get(zk_replica_path + "/host"));
        if (interserver_scheme != address.scheme)
            throw Exception(
                "Interserver schemes are different '" + interserver_scheme + "' != '" + address.scheme + "', can't fetch part from "
                    + address.host,
                ErrorCodes::LOGICAL_ERROR);

        if (!part_name.empty())
        {
            part = fetcher.fetchPart(
                getInMemoryMetadataPtr(),
                getContext(),
                part_name,
                zk_replica_path,
                address.host,
                address.replication_port,
                timeouts,
                credentials->getUser(),
                credentials->getPassword(),
                interserver_scheme,
                throttler);

            updated_parts.emplace(part);
        }
        auto new_deletes = fetcher.fetchDeleteFiles(
            getInMemoryMetadataPtr(),
            getContext(),
            updated_parts,
            version,
            zk_replica_path,
            address.host,
            address.replication_port,
            timeouts,
            credentials->getUser(),
            credentials->getPassword(),
            interserver_scheme,
            throttler);

        /// Get locks for blocking dictionary
        // auto dict_locks = getDictionaryLocks();

        SyncPartsTransaction local_txn(*this, new_deletes, version);
        if (part)
            local_txn.preCommit(part);
        local_txn.commit();

        for (auto & updated_part : updated_parts)
            const_cast<IMergeTreeDataPart &>(*updated_part).gcUniqueIndexIfNeeded();

        if (part)
        {
            /// TODO sanity check entry.removed_part are removed and add them to replace_parts

            /// Recode the data part which is fetched from remote side
            // if (getContext()->getSettingsRef().enable_sync_fetch)
            //     recodeDataPart(part);
            // else
            //     addPartForRecode(part);

            // if (getContext()->getSettingsRef().enable_async_build_bitmap_in_attach)
            //     addPartForBitMapIndex(part);
        }
        write_part_log({});
    }
    catch (...)
    {
        /// if error happens while fetching delete files, need to remove temp directory
        /// otherwise fetchPart will throw DIRECTORY_ALREADY_EXISTS error on the next try
        if (part && part->is_temp)
            part->remove();
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }
    if (!part_name.empty())
        LOG_DEBUG(log, "Fetched part {} and {} delete files from {}", part_name, updated_parts.size(), replica);
    else
        LOG_DEBUG(log, "Fetched {} delete files from {}", updated_parts.size(), replica);
}

void StorageHaUniqueMergeTree::executeResetManifest(const ManifestLogEntry & entry)
{
    auto reset_version = entry.target_version;
    if (!execute_reset_manifest[entry.version])
    {
        /// sanity check
        if (reset_version != manifest_store->commitVersion())
            throw Exception(
                "Invalid state. RESET_MANIFEST to " + toString(reset_version) + " is skipped but manifest is "
                    + manifest_store->stateSummary(),
                ErrorCodes::LOGICAL_ERROR);
        return;
    }

    try
    {
        auto lock = lockParts();
        resetUniqueTableToVersion(lock, reset_version);
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to reset table to {}, set state to BROKEN: {}", reset_version, getCurrentExceptionMessage(false));
        setUniqueTableState(UniqueTableState::BROKEN);
        restarting_thread.wakeup();
        throw;
    }
    calculateColumnSizesImpl();
    // recalculateColumnSizes(); /// note that it would acquire parts lock so be aware of deadlock
    // dropQueryCache();
}

void StorageHaUniqueMergeTree::executeCheckpointManifest(const ManifestLogEntry & entry)
{
    manifest_store->checkpointTo(entry.target_version);
}

/// leader-only
void StorageHaUniqueMergeTree::checkpointLogTask()
{
    if (getUniqueTableState() != UniqueTableState::NORMAL)
        return;

    /// Checkpoint to version V if and only if
    /// 1. All replica's commit version >= V. For failed replica, we read its commit version from ZK (it's OK to read an older version)
    /// 2. We as the leader contain the log for V. It's required to avoid checkpointing to a version that's already discarded by the current
    ///    leader, for example:
    ///    T1: r1 is the leader and logs for each replica are
    ///        r1: 10 11 12(commit)
    ///        r2: 10(commit) 11
    ///    T2: r1 crashes and r2 becomes leader and adds more logs
    ///        r2: 10 13(reset manifest to 10) 14 15(commit)
    ///    T3: r1 restarts and before it resets manifest to 10, the smallest commit version is 12 but it's no longer present on leader
    ///        r1: 10 11 12(commit)
    ///        r2: 10 13 14 15(commit)
    /// By following the above rules, it's guaranteed that all logs in the checkpoint are committed on all replicas and would never
    /// be discarded by RESET_MANIFEST log.

    auto settings = getSettings();
    SCOPE_EXIT({ checkpoint_log_task->scheduleAfter(settings->ha_unique_checkpoint_attempt_interval * 1000); });

    auto zookeeper = getZooKeeper();
    if (/*!getContext()->isListenPortsReady() || */ !is_leader)
        return;

    try
    {
        UInt64 cluster_commit_version = unique_commit_version.load(std::memory_order_acquire);
        UInt64 min_checkpoint_version = manifest_store->checkpointVersion() + settings->ha_unique_checkpoint_min_logs;
        if (cluster_commit_version < min_checkpoint_version)
        {
            LOG_TRACE(
                log,
                "retry checkpoint next time, cluster_commit_version {} < min_checkpoint_version {}",
                cluster_commit_version,
                min_checkpoint_version);
            return;
        }
        if (!manifest_store->containsLog(cluster_commit_version))
        {
            LOG_DEBUG(
                log,
                "retry checkpoint next time, log at cluster_commit_version {} not found, manifest: {}",
                cluster_commit_version,
                manifest_store->stateSummary());
            return;
        }
        LOG_INFO(log, "Checkpointing to {}, manifest: {}", cluster_commit_version, manifest_store->stateSummary());
        manifest_store->checkpointTo(cluster_commit_version);

        /// write CHECKPOINT_MANIFEST log
        auto manifest_lock = manifest_store->writeLock();
        auto version = allocLSNAndSet(zookeeper);
        ManifestLogEntry entry;
        entry.type = ManifestLogEntry::CHECKPOINT_MANIFEST;
        entry.version = version;
        entry.prev_version = manifest_store->latestVersion();
        entry.source_replica = replica_name;
        entry.target_version = cluster_commit_version;
        manifest_store->append(manifest_lock, entry, /*commit=*/true);
        LOG_INFO(log, "Successfully checkpoint to {}, manifest: {}", cluster_commit_version, manifest_store->stateSummary());
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

struct MergingResourceHolder
{
    const FutureMergedMutatedPart & future_part;
    StorageHaUniqueMergeTree & storage;
    bool need_clean_merge_state;
    ReservationPtr reserved_space;

    MergingResourceHolder(const FutureMergedMutatedPart & future_part_, StorageHaUniqueMergeTree & storage_)
        : future_part(future_part_), storage(storage_), need_clean_merge_state(false)
    {
        auto total_size = MergeTreeDataMergerMutator::estimateNeededDiskSpace(future_part.parts);
        reserved_space = storage.reserveSpace(total_size);
        if (!reserved_space)
            throw Exception("Not enough space for merging parts", ErrorCodes::NOT_ENOUGH_SPACE);
    }

    ~MergingResourceHolder()
    {
        {
            auto lock = storage.mergeSelectLock(/*acquire=*/true);
            for (auto & part : future_part.parts)
                storage.parts_under_merge.erase(part);
        }
        /// could happen when exception was thrown when merging new part
        if (need_clean_merge_state)
        {
            auto write_lock = storage.uniqueWriteLock();
            for (auto & part : future_part.parts)
                storage.running_merge_states.erase(part);
        }
    }
};

bool StorageHaUniqueMergeTree::doMerge(bool aggressive, const String & partition_id, bool final, bool enable_try)
{
    auto zookeeper = getZooKeeper();
    if (!zookeeper || zookeeper->expired() || !is_leader)
    {
        if (enable_try)
            LOG_DEBUG(log, "Skip merge because we are no longer leader");
        return false;
    }
    /// We leverage the fact that parts are multi-versioned to divide the merge process into several phases.
    /// Phase 1: select the best candidate to merge.
    ///     Only one thread can do merge selecting at a time. A part can't be selected by multiple merge tasks.
    /// Phase 2: wait for running inserts/merges to finish and then capture delete bitmap snapshot of input parts.
    /// Phase 3: merge input parts into temporary part.
    ///     During this period, concurrent inserts may delete rows from the input parts. To handle this, the merge thread
    ///     maintains a delete buffer. Inserts started after phase 2 not only create new versions of delete bitmap files
    ///     for old parts, but also insert the deleted PKs to the delete buffer.
    ///     The running merge will be cancelled if the size of the delete buffer exceeds certain threshold because it means
    ///     there are lots of deletes after phase 3 starts and it would be beneficial to schedule this merge later.
    /// Phase 4: process incremental deletes during phase 3 and then commit the new part.
    ///
    /// Phase 1 and 3 allow concurrent inserts while phase 2 and 4 block inserts. Therefore it's important to make
    /// execution time of phase 2 and 4 as short as possible.
    ///
    /// NOTE on lock order: merge select lock -> table structure lock -> unique write lock
    ///
    /// NOTE ttl work will also be done in MergeTreeDataMergeMutator class, see selectPartsToMerge for more detail.

    /// Phase 1: select parts to merge.
    FutureMergedMutatedPart future_part(merging_params);
    {
        auto lock = mergeSelectLock(/*acquire=*/false);
        if (aggressive)
            lock.lock();
        else if (!lock.try_lock())
            return false;
        /// FIXME (UNIQUE KEY): futher verify ttl merge or ttl worker
        /// TTLWorker(lock, zookeeper);
        auto table_lock = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

        auto can_merge = [this] (const DataPartPtr & left, const DataPartPtr & right, bool)
        {
            return !parts_under_merge.count(left) && !parts_under_merge.count(right);
        };
        SelectPartsDecision selected = SelectPartsDecision::NOTHING_TO_MERGE;
        if (partition_id.empty())
        {
            /// FIXME (UNIQUE KEY): revisit after merge task is running in processing pool
            size_t running_bg_merges = CurrentMetrics::values[CurrentMetrics::BackgroundUniqueTableSchedulePoolTask].load(std::memory_order_relaxed);
            UInt64 max_source_parts_size = merger_mutator.getMaxSourcePartsSizeForMerge(
                /*pool_size=*/ getContext()->getUniqueTableSchedulePool().getNumberOfThreads(),
                /*pool_used=*/ running_bg_merges == 0 ? 0 : running_bg_merges - 1);
            if (max_source_parts_size > 0)
                selected = merger_mutator.selectPartsToMerge(future_part, aggressive, max_source_parts_size, can_merge, /*merge_with_ttl_allowed*/true, nullptr);
            else if (enable_try)
                LOG_DEBUG(log, "Current value of max_source_parts_size is zero");
        }
        else
        {
            UInt64 disk_space = getStoragePolicy()->getMaxUnreservedFreeSpace();
            selected = merger_mutator.selectAllPartsToMergeWithinPartition(future_part, disk_space, can_merge, partition_id, final, getInMemoryMetadataPtr(), nullptr, /*optimize_skip_merged_partitions*/ false);
        }
        if (selected != SelectPartsDecision::SELECTED || enable_try)
            return false;

        for (auto & part : future_part.parts)
            parts_under_merge.insert(part);
    }

    /// FIXME (UNIQUE KEY): unique table's part has to be in wide format.
    future_part.type = MergeTreeDataPartType::WIDE;

    MergingResourceHolder resource_holder(future_part, *this);
    auto merge_state = std::make_shared<UniqueMergeState>();
    merge_state->new_part_name = future_part.name;
    auto table_lock = lockForShare(RWLockImpl::NO_QUERY, getContext()->getSettingsRef().lock_acquire_timeout);

    UInt64 block_insert_millis = 0;
    /// Phase 2: wait for running inserts/merges to finish and then capture delete bitmap snapshot of input parts.
    {
        auto write_lock = uniqueWriteLock();
        Stopwatch block_insert_watch;
        /// fallback to drop parts if all selected parts became empty
        bool all_parts_are_empty = std::all_of(future_part.parts.begin(), future_part.parts.end(),
                                               [](auto & part) { return part->rows_count == part->numDeletedRows(); });
        if (all_parts_are_empty)
        {
            leaderDropParts(zookeeper, future_part.parts, write_lock);
            return true;
        }

        {
            /// Some other places might acquire write_lock before phase 2 starts and change part's state, we should double
            /// check here. for example in truncate table case, see `truncate` function for more details.
            auto parts_lock = lockParts();
            for (auto & part : future_part.parts)
            {
                if (!part->checkState({DataPartState::Committed}))
                {
                    /// Stop doing merge process and return.
                    return false;
                }
            }
        }

        future_part.delete_snapshot = getLatestDeleteSnapshot(future_part.parts);
        /// add merge state so that future inserts can be aware of this merge task
        resource_holder.need_clean_merge_state = true;
        for (auto & part : future_part.parts)
            running_merge_states.insert({part, merge_state});
        block_insert_millis += block_insert_watch.elapsedMilliseconds();
    }

    Stopwatch stopwatch;
    MutableDataPartPtr new_part;
    auto write_part_log = [&] (const ExecutionStatus & execution_status,
                               const FutureMergedMutatedPart & future_merged_part,
                               const MergeList::EntryPtr & merge_entry)
    {
        writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            stopwatch.elapsed(),
            future_merged_part.name,
            new_part,
            future_merged_part.parts,
            merge_entry.get());
    };

    MergeList::EntryPtr merge_entry = getContext()->getMergeList().insert(getStorageID(), future_part);

    try
    {
        /// Phase 3: merge input parts into temporary part.
        new_part = merger_mutator.mergePartsToTemporaryPart(
            future_part,
            getInMemoryMetadataPtr(),
            *merge_entry,
            table_lock,
            time(nullptr),
            getContext(),
            resource_holder.reserved_space,
            /*deduplicate*/ false,
            /*deduplicate_by_columns*/{},
            merging_params,
            /*parent*/nullptr,
            /*prefix*/"",
            &(merge_state->blocker));

        /// Phase 4: process incremental deletes during phase 3 and then commit the new part.
        {
            auto write_lock = uniqueWriteLock();
            Stopwatch block_insert_watch;

            if (merge_state->blocker.isCancelled())
            {
                new_part->remove();
                throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);
            }
            /// convert delete buffer into delete bitmap for new part
            MutableDeleteBitmapPtr delete_bitmap(new Roaring);
            UniqueKeyIndexPtr unique_key_index = new_part->getUniqueKeyIndex();
            for (auto & deleted_key : merge_state->delete_buffer)
            {
                UInt32 rowid;
                if (!new_part->getValueFromUniqueIndex(unique_key_index, deleted_key, rowid))
                    throw Exception("Merged part " + new_part->name + " doesn't contain key in delete buffer", ErrorCodes::LOGICAL_ERROR);
                delete_bitmap->add(rowid);
            }

            /// cleanup merge state
            merge_state->delete_buffer.clear();
            for (auto & part : future_part.parts)
                running_merge_states.erase(part);
            resource_holder.need_clean_merge_state = false;

            auto manifest_lock = manifest_store->writeLock();
            /// allocate LSN
            Coordination::Requests ops;
            auto [lsn, set_lsn_request] = allocLSNAndMakeSetRequest(zookeeper);
            ops.emplace_back(std::move(set_lsn_request));

            /// pre-commit new part
            new_part->writeDeleteFileWithVersion(delete_bitmap, lsn);
            DataPartsDeleteSnapshot new_deletes;
            new_deletes.emplace(new_part, delete_bitmap);
            MergeTreeData::SyncPartsTransaction local_txn(*this, new_deletes, lsn);
            auto replaced_parts = local_txn.preCommit(new_part);

            /// sanity check
            if (replaced_parts.size() != future_part.parts.size())
                throw Exception("Adding part " + new_part->name + " should replace " + toString(future_part.parts.size())
                                + " parts, but got " + toString(replaced_parts.size()), ErrorCodes::LOGICAL_ERROR);
            for (size_t i = 0; i < future_part.parts.size(); ++i)
                if (future_part.parts[i]->name != replaced_parts[i]->name)
                    throw Exception("Unexpected part removed when adding " + new_part->name + ": " + replaced_parts[i]->name
                                    + " instead of " + future_part.parts[i]->name, ErrorCodes::LOGICAL_ERROR);

            /// update /latest_lsn
            Coordination::Responses responses;
            auto multi_code = zookeeper->tryMultiNoThrow(ops, responses);
            if (multi_code != Coordination::Error::ZOK)
            {
                local_txn.rollback();
                String errmsg = "Unexpected ZK error while updating /latest_lsn to " + toString(lsn) + " : "
                                + Coordination::errorMessage(multi_code);
                throw Exception(errmsg, ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR);
            }

            /// write manifest log and commit new part
            ManifestLogEntry entry;
            entry.type = ManifestLogEntry::SYNC_PARTS;
            entry.version = lsn;
            entry.prev_version = manifest_store->latestVersion();
            entry.source_replica = replica_name;
            entry.added_parts.push_back(new_part->name);
            for (auto & part : future_part.parts)
                entry.removed_parts.push_back(part->name);
            manifest_store->append(manifest_lock, entry, /*commit=*/true);
            local_txn.commit();
            write_part_log({}, future_part, merge_entry);

            std::ostringstream oss;
            for (auto & part : future_part.parts)
                oss << part->name << " ";
            block_insert_millis += block_insert_watch.elapsedMilliseconds();
            LOG_DEBUG(log, "Merged {} parts: {}to {} at version{}, blocking insert for {} ms", future_part.parts.size(), oss.str(), new_part->name, lsn, block_insert_millis);
        }
        new_part->gcUniqueIndexIfNeeded();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ABORTED)
        {
            LOG_DEBUG(log, "Merging of part {} is aborted", future_part.name);
            return false;
        }
        write_part_log(ExecutionStatus::fromCurrentException(), future_part, merge_entry);
        throw;
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException(), future_part, merge_entry);
        throw;
    }
    // dropQueryCache();
    return true;
}

/// note that multiple threads from background pool could run merge task concurrently.
bool StorageHaUniqueMergeTree::mergeTask()
{
    try
    {
        if (getUniqueTableState() != UniqueTableState::NORMAL)
            return false;

        doMerge(/*aggressive=*/false, /*partition_id=*/"", /*final=*/false, /*enable_try=*/false);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
    // TODO: Pitfall: only one threds run merge task and the schedule time is fixed.
    merge_task_handle->scheduleAfter(1 * 1000);
    return false;
}

bool StorageHaUniqueMergeTree::optimize(const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    ContextPtr query_context)
{
    assertNotReadonly();
    if (!is_leader /*&& !enable_try*/)
    {
        if (sendRequestToLeaderReplica(query, query_context))
            return true;
    }

    if (!partition && final)
    {
        /// for "optimize table {name} final", trying to merge all parts for all partitions
        DataPartsVector data_parts = getDataPartsVector();
        std::unordered_set<String> partition_ids;
        for (const DataPartPtr & part : data_parts)
            partition_ids.emplace(part->info.partition_id);
        for (const String & partition_id : partition_ids)
        {
            if (!doMerge(/*aggressive=*/true, partition_id, final, /*enable_try*/false))
                return false;
        }
    }
    else
    {
        String partition_id;
        if (partition)
            partition_id = getPartitionIDFromQuery(partition, query_context);

        if (!doMerge(/*aggressive=*/true, partition_id, final, /*enable_try*/false))
            return false;
    }
    return true;
}

/// TODO add remove part log?
void StorageHaUniqueMergeTree::leaderDropParts(
    zkutil::ZooKeeperPtr & zookeeper, const DataPartsVector & parts_to_drop, const UniqueWriteLock & lock)
{
    if (parts_to_drop.empty())
        return;

    if (!lock.owns_lock())
    {
        throw Exception("Must own unique write lock before dropping any parts to avoid data race", ErrorCodes::LOGICAL_ERROR);
    }

    if (!is_leader)
        throw Exception("Only leader can drop parts", ErrorCodes::READONLY);

    auto manifest_lock = manifest_store->writeLock();
    auto version = allocLSNAndSet(zookeeper);

    /// drop parts locally
    removePartsFromWorkingSet(parts_to_drop, true);

    // recalculateMapKeyCacheByParts(parts_to_drop);

    // TODO: check caches later
    // getContext()->dropCaches(getStorageID().database_name, getStorageID().table_name);

    ManifestLogEntry entry;
    entry.type = ManifestLogEntry::SYNC_PARTS;
    entry.version = version;
    entry.prev_version = manifest_store->latestVersion();
    entry.source_replica = replica_name;
    for (auto & part : parts_to_drop)
        entry.removed_parts.push_back(part->name);
    manifest_store->append(manifest_lock, entry, /*commit=*/true);
}

void StorageHaUniqueMergeTree::leaderDeatchParts(
    zkutil::ZooKeeperPtr & zookeeper, const DataPartsVector & parts_to_drop, const UniqueWriteLock & lock)
{
    if (parts_to_drop.empty())
        return;

    if (!lock.owns_lock())
    {
        throw Exception("Must own unique write lock before deatching any parts to avoid data race", ErrorCodes::LOGICAL_ERROR);
    }

    if (!is_leader)
        throw Exception("Only leader can Deatch parts", ErrorCodes::READONLY);

    auto manifest_lock = manifest_store->writeLock();
    auto version = allocLSNAndSet(zookeeper);

    removePartsFromWorkingSet(parts_to_drop, true);

    for (auto & part : parts_to_drop)
        const_cast<IMergeTreeDataPart &>(*part).loadChecksumsIfNeed();

    // recalculateMapKeyCacheByParts(parts_to_drop);
    // getContext()->dropCaches(database_name, table_name);

    for (const auto & part : parts_to_drop)
    {
        LOG_INFO(log, "Leader detaching {}", part->relative_path);
        part->makeCloneInDetached("", getInMemoryMetadataPtr());
    }

    ManifestLogEntry entry;
    entry.type = ManifestLogEntry::DETACH_PARTS;
    entry.version = version;
    entry.prev_version = manifest_store->latestVersion();
    entry.source_replica = replica_name;
    for (auto & part : parts_to_drop)
    {
        entry.removed_parts.push_back(part->name);
    }
    manifest_store->append(manifest_lock, entry, /*commit=*/true);
}

String StorageHaUniqueMergeTree::activeLeaderReplicaName() const
{
    if (is_leader)
        return replica_name;
    auto leader_name = getCachedLeader();
    if (!leader_name.empty() && log_exchanger.isActiveReplica(leader_name))
        return leader_name;
    return {};
}

ManifestStore::LogEntries StorageHaUniqueMergeTree::getManifestLogs(UInt64 & committed_version)
{
    UInt64 from = manifest_store->checkpointVersion();
    auto logs = manifest_store->getLogEntries(from);
    committed_version = manifest_store->commitVersion();
    return logs;
}

void StorageHaUniqueMergeTree::getStatus(Status & res, bool with_zk_fields)
{
    auto zookeeper = tryGetZooKeeper();
    res.is_leader = is_leader;
    res.is_readonly = is_readonly;
    res.is_session_expired = !zookeeper || zookeeper->expired();
    res.status = toString(getUniqueTableState());
    res.cached_leader = getCachedLeader();
    res.zookeeper_path = zookeeper_path;
    res.replica_name = replica_name;
    res.replica_path = replica_path;
    res.absolute_delay = getAbsoluteDelay();
    res.checkpoint_lsn = manifest_store->checkpointVersion();
    res.commit_lsn = manifest_store->commitVersion();
    res.latest_lsn = manifest_store->latestVersion();
    res.cluster_commit_lsn = unique_commit_version;
    if (res.is_session_expired || !with_zk_fields)
    {
        res.latest_lsn = 0;
        res.total_replicas = 0;
        res.active_replicas = 0;
    }
    else
    {
        res.cluster_latest_lsn = parse<UInt64>(zookeeper->get(getZKLatestLSNPath()));
        auto all_replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
        res.total_replicas = all_replicas.size();
        res.active_replicas = 0;
        for (const String & replica : all_replicas)
            if (zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
                ++res.active_replicas;
    }
}

std::optional<Cluster::Address> StorageHaUniqueMergeTree::findClusterAddress(const HaMergeTreeAddress & target) const
{
    for (const auto & name_and_cluster : getContext()->getClusters()->getContainer())
    {
        const ClusterPtr & cluster = name_and_cluster.second;
        const auto & shards_info = cluster->getShardsInfo();
        const auto & addresses_with_failover = cluster->getShardsAddresses();

        for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
        {
            const auto & shard_addresses = addresses_with_failover[shard_index];

            for (size_t replica_index = 0; replica_index < shard_addresses.size(); ++replica_index)
            {
                const Cluster::Address & address = shard_addresses[replica_index];
                /// user is actually specified, not default
                if (address.host_name == target.host && address.port == leader_address.queries_port && address.user_specified)
                    return address;
            }
        }
    }
    return {};
}

bool StorageHaUniqueMergeTree::sendRequestToLeaderReplica(const ASTPtr & query, ContextPtr query_context)
{
    HaMergeTreeAddress addr;
    auto leader_name = getCachedLeader(&addr);
    if (leader_name.empty())
        throw Exception("Can't forward to leader because leader is unknown right now", ErrorCodes::READONLY);

    auto new_query = query->clone();
    if (auto * alter = typeid_cast<ASTAlterQuery *>(new_query.get()))
    {
        alter->database = addr.database;
        alter->table = addr.table;
    }
    else if (auto * truncate = typeid_cast<ASTDropQuery *>(new_query.get()))
    {
        if (truncate->kind != ASTDropQuery::Truncate)
        {
            throw Exception("Can't proxy this query. only support truncate query", ErrorCodes::NOT_IMPLEMENTED);
        }
        truncate->database = addr.database;
        truncate->table = addr.table;
    }
    else if (auto * optimize = typeid_cast<ASTOptimizeQuery *>(new_query.get()))
    {
        optimize->database = addr.database;
        optimize->table = addr.table;
    }
    else
        throw Exception("Can't proxy this query. Unsupported query type", ErrorCodes::NOT_IMPLEMENTED);

    // const auto & query_settings = query_context->getSettingsRef();
    const auto & query_client_info = query_context->getClientInfo();
    String user = query_client_info.current_user;
    String password;
    if (auto address = findClusterAddress(addr); address)
    {
        user = address->user;
        password = address->password;
    }

    Connection connection(
        addr.host,
        addr.queries_port,
        addr.database,
        user,
        password,
        /*cluster*/"",
        /*cluster_secret*/"",
        /*client_name=*/"Follower replica",
        Protocol::Compression::Enable,
        Protocol::Secure::Disable);

    WriteBufferFromOwnString new_query_ss;
    formatAST(*new_query, new_query_ss, false, true);
    RemoteBlockInputStream stream(connection, new_query_ss.str(), {}, getContext());
    NullBlockOutputStream output({});
    copyData(stream, output);
    return true;
}

bool StorageHaUniqueMergeTree::isSelfLost()
{
    return "1" == getZooKeeper()->get(replica_path + "/is_lost");
}

void StorageHaUniqueMergeTree::markSelfLost()
{
    auto zookeeper = getZooKeeper();
    /// Maybe someone has already marked it.
    if ("1" == zookeeper->get(replica_path + "/is_lost"))
        return;

    std::map<String, Coordination::Stat> alive_is_lost_stats;
    bool has_alive_replica = false;

    for (const String & replica : zookeeper->getChildren(zookeeper_path + "/replicas"))
    {
        if (replica == replica_name) continue;
        Coordination::Stat is_lost_stat;
        String replica_is_lost_value = zookeeper->get(zookeeper_path + "/replicas/" + replica + "/is_lost", &is_lost_stat);
        if ("0" == replica_is_lost_value)
        {
            alive_is_lost_stats[replica] = is_lost_stat;
            has_alive_replica = true;
        }
    }

    if (!has_alive_replica)
        throw Exception("This is the last alive replica, cannot mark self lost", ErrorCodes::TOO_FEW_LIVE_REPLICAS);

    Coordination::Stat host_stat;
    zookeeper->get(replica_path + "/host", &host_stat);

    Coordination::Requests ops;
    for (auto & [replica, is_lost_stat] : alive_is_lost_stats)
        ops.push_back(zkutil::makeCheckRequest(zookeeper_path + "/replicas/" + replica + "/is_lost", is_lost_stat.version));
    ops.push_back(zkutil::makeCheckRequest(replica_path + "/host", host_stat.version));
    ops.push_back(zkutil::makeSetRequest(replica_path + "/is_lost", "1", -1));

    Coordination::Responses resps;
    auto code = zookeeper->tryMulti(ops, resps);
    if (code == Coordination::Error::ZBADVERSION)
        throw Exception(replica_name + " became active when we marked self lost.", ErrorCodes::REPLICA_STATUS_CHANGED);
    else
        zkutil::KeeperMultiException::check(code, ops, resps);

    LOG_DEBUG(log, "Mark {} as lost replica.", replica_name);
}

void StorageHaUniqueMergeTree::markSelfNonLost()
{
    auto zookeeper = getZooKeeper();
    if ("0" == zookeeper->get(replica_path + "/is_lost"))
        return;
    zookeeper->set(replica_path + "/is_lost", "0");
    LOG_DEBUG(log, "Mark {} as non-lost replica.", replica_name);
}

void StorageHaUniqueMergeTree::saveRepairVersionToZk(UInt64 repair_version)
{
    auto path = replica_path + "/repair_lsn";
    auto zookeeper = getZooKeeper();
    String data;
    Coordination::Stat stat;
    bool has_repair_version = zookeeper->tryGet(path, data, &stat);
    if (!has_repair_version)
    {
        zookeeper->create(path, toString(repair_version), zkutil::CreateMode::Ephemeral);
        LOG_TRACE(log, "created {}, value = {}", path, repair_version);
    }
    else if (toString(repair_version) != data)
    {
        zookeeper->set(path, toString(repair_version), stat.version);
        LOG_TRACE(log, "updated {} from {} to {}", path, data, repair_version);
    }
}

void StorageHaUniqueMergeTree::removeRepairVersionFromZk()
{
    auto path = replica_path + "/repair_lsn";
    auto zookeeper = getZooKeeper();
    String data;
    Coordination::Stat stat;
    if (zookeeper->tryGet(path, data, &stat))
    {
        zookeeper->remove(path, stat.version);
        LOG_TRACE(log, "removed {}", path);
    }
}

void StorageHaUniqueMergeTree::enterRepairMode()
{
    if (getUniqueTableState() != UniqueTableState::REPAIR_MANIFEST)
        throw Exception("Unexpected state for enterRepairMode: " + toString(getUniqueTableState()), ErrorCodes::LOGICAL_ERROR);

    LOG_INFO(log, "Enter repair mode...");
    /// 1. found leader replica and its manifest info
    auto alive_replica_infos = log_exchanger.getAllManifestStatus();
    String leader_replica_name;
    UInt64 leader_checkpoint_version = 0;
    UInt64 leader_commit_version = 0;
    for (auto & info : alive_replica_infos)
    {
        if (info.second.is_leader)
        {
            leader_replica_name = info.first;
            leader_checkpoint_version = info.second.checkpoint_version;
            leader_commit_version = info.second.commit_version;
            break;
        }
    }
    if (leader_replica_name.empty())
    {
        LOG_TRACE(log, "No leader found, retry repair later");
        return;
    }
    if (leader_commit_version == 0 && manifest_store->commitVersion() == 0)
    {
        /// table is empty, nothing to repair
        setUniqueTableState(UniqueTableState::REPAIRED);
        restarting_thread.wakeup();
        return;
    }

    /// 2. set repair version so that leader won't checkpoint after repair version
    saveRepairVersionToZk(leader_checkpoint_version);

    /// 3. repair manifest to be the same with leader
    auto snapshot = log_exchanger.getManifestSnapshot(leader_replica_name, leader_checkpoint_version);
    manifest_store->resetToCheckpoint(leader_checkpoint_version, snapshot);
    if (leader_commit_version > leader_checkpoint_version)
    {
        auto from = leader_checkpoint_version + 1;
        auto limit = leader_commit_version - leader_checkpoint_version;
        auto entries = log_exchanger.getLogEntries(leader_replica_name, from, limit);
        /// create snapshot at repair version and remove entry beyond repair version
        bool log_is_complete = false;
        for (size_t i = 0; i < entries.size(); ++i)
        {
            auto & value = entries[i];
            if (value.version <= leader_commit_version && value.type == ManifestLogEntry::SYNC_PARTS)
            {
                for (auto & part : value.added_parts)
                    snapshot.parts[part] = value.version;
                for (auto & part : value.updated_parts)
                    snapshot.parts[part] = value.version;
                for (auto & part : value.removed_parts)
                    snapshot.parts.erase(part);
            }
            if (value.version == leader_commit_version)
            {
                log_is_complete = true;
                entries.resize(i + 1);
                break;
            }
        }
        if (!log_is_complete)
        {
            std::ostringstream oss;
            oss << "Fetched logs from " << leader_replica_name << " (from=" << from << ", limit=" << limit
                << ") doesn't contain log at repair version " << leader_commit_version << ": #logs=" << entries.size();
            if (!entries.empty())
                oss << ", first=" << entries.front().version << ", last=" << entries.back().version;
            throw Exception(oss.str(), ErrorCodes::LOGICAL_ERROR);
        }

        auto manifest_lock = manifest_store->writeLock();
        manifest_store->append(manifest_lock, entries, /*commit=*/true);
    }
    manifest_store->clearAllPartRemovedVersion();
    LOG_INFO(
        log,
        "Manifest has been repaired to {}, containing {} parts, with latest schema: {}\n{}",
        leader_commit_version,
        snapshot.parts.size(),
        manifest_store->getLatestTableMetadata(),
        manifest_store->getLatestTableColumns());

    /// 4. mark all parts not in snapshot as outdated so that they won't prevent a part in snapshot from commit
    auto parts_lock = lockParts();
    Strings removed_parts;
    for (auto it = data_parts_by_state_and_info.begin(); it != data_parts_by_state_and_info.end();)
    {
        auto next = std::next(it);
        if (snapshot.parts.count((*it)->name) == 0)
        {
            removed_parts.emplace_back((*it)->name);
            LOG_INFO(log, "Removing part {} because it's not in the snapshot", (*it)->name);
            (*it)->remove_time.store((*it)->modification_time, std::memory_order_relaxed);
            modifyPartState(it, DataPartState::Outdated);
        }
        it = next;
    }
    manifest_store->putPartRemovedVersion(removed_parts, leader_commit_version);

    /// 5. determine sets of parts and delete files to fetch
    RepairStatePtr new_repair_state = std::make_unique<RepairState>();
    new_repair_state->version = leader_commit_version;
    new_repair_state->leader = leader_replica_name;
    new_repair_state->start_time = time(nullptr);

    for (auto & entry : snapshot.parts)
    {
        auto part_info = MergeTreePartInfo::fromPartName(entry.first, format_version);
        auto it = data_parts_by_info.find(part_info);
        if (it == data_parts_by_info.end())
        {
            new_repair_state->parts_to_fetch.emplace_back(entry.first, entry.second);
            continue;
        }

        auto part = *it;
        if (!part)
        {
            // NOTE: If this part is broken, then we will get a nullptr from metastore

            // First drop this part from metastore
            // drop(it);

            // Then try to fetch from leader
            new_repair_state->parts_to_fetch.emplace_back(entry.first, entry.second);
            continue;
        }

        /// try to load the desired delete bitmap
        if (part->deleteBitmapVersion() != entry.second)
        {
            try
            {
                auto delete_bitmap = part->readDeleteFileWithVersion(entry.second, /*log_on_error=*/false);
                part->setDeleteBitmapWithVersion(delete_bitmap, entry.second);
            }
            catch (...)
            {
                /// ignore error
            }
        }

        if (part->deleteBitmapVersion() == entry.second)
        {
            part->remove_time.store(0, std::memory_order_relaxed);
            modifyPartState(part, DataPartState::Committed);
        }
        /// for non-merged part, name uniquely identifies part's data, only needs to fetch delete file
        else if (part->info.level == 0)
        {
            part->remove_time.store(0, std::memory_order_relaxed);
            modifyPartState(part, DataPartState::Committed);
            new_repair_state->delete_files_to_fetch.emplace_back(part, entry.second);
        }
        /// for merged part, different parts may have the same name, therefore needs to re-fetch part.
        else
        {
            /// set to outdated so that fetchAndLoadData can delete it before fetch
            modifyPartState(it, DataPartState::Outdated);
            new_repair_state->parts_to_fetch.emplace_back(entry.first, entry.second);
        }
    }

    new_repair_state->total = new_repair_state->delete_files_to_fetch.size() + new_repair_state->parts_to_fetch.size();
    if (new_repair_state->total == 0)
    {
        LOG_INFO(log, "Found nothing to repair");
        try
        {
            resetUniqueTableToVersion(parts_lock, leader_commit_version);
            setUniqueTableState(UniqueTableState::REPAIRED);
            restarting_thread.wakeup();
        }
        catch (...)
        {
            LOG_ERROR(log, "Can't reset table to version {}: {}", leader_commit_version, getCurrentExceptionMessage(false));
        }
    }
    else
    {
        LOG_INFO(log, "Found {} parts to repair out of {} parts in snapshot", new_repair_state->total, snapshot.parts.size());
        current_repair_state = std::move(new_repair_state);
        setUniqueTableState(UniqueTableState::REPAIR_DATA);
        repair_data_task->activateAndSchedule();
    }
}

void StorageHaUniqueMergeTree::repairDataTask()
{
    if (getUniqueTableState() != UniqueTableState::REPAIR_DATA)
        throw Exception("Unexpected state " + toString(getUniqueTableState()) + " for repairDataTask", ErrorCodes::LOGICAL_ERROR);
    if (current_repair_state == nullptr)
        throw Exception("current_repair_state is null for repairDataTask", ErrorCodes::LOGICAL_ERROR);

    auto & state = current_repair_state;
    auto settings = getSettings();
    String part_name;
    try
    {
        if (!state->delete_files_to_fetch.empty())
        {
            auto [part, delete_version] = state->delete_files_to_fetch.back();
            part_name = part->name;
            auto from_replica = chooseReplicaToFetch(delete_version);

            auto timeouts = ConnectionTimeouts::getHTTPTimeouts(getContext());
            auto credentials = getContext()->getInterserverCredentials();
            String interserver_scheme = getContext()->getInterserverScheme();
            String from_replica_path = zookeeper_path + "/replicas/" + from_replica;

            HaMergeTreeAddress address(getZooKeeper()->get(from_replica_path + "/host"));
            if (interserver_scheme != address.scheme)
                throw Exception(
                    "Interserver schemes are different '" + interserver_scheme + "' != '" + address.scheme + "', can't fetch part from "
                        + address.host,
                    ErrorCodes::LOGICAL_ERROR);

            auto new_deletes = fetcher.fetchDeleteFiles(
                getInMemoryMetadataPtr(),
                getContext(),
                {part},
                delete_version,
                from_replica_path,
                address.host,
                address.replication_port,
                timeouts,
                credentials->getUser(),
                credentials->getPassword(),
                interserver_scheme,
                createThrottler(getContext()));
            if (new_deletes.find(part) == new_deletes.end())
                throw Exception("Server didn't return delete file of the requested part " + part_name, ErrorCodes::LOGICAL_ERROR);
            part->setDeleteBitmapWithVersion(new_deletes[part], delete_version);

            state->delete_files_to_fetch.pop_back();
        }
        else if (!state->parts_to_fetch.empty())
        {
            auto [part_name, delete_version] = state->parts_to_fetch.back();
            auto from_replica = chooseReplicaToFetch(delete_version);
            fetchAndLoadData(from_replica, part_name, {}, delete_version, createThrottler(getContext()));
            state->parts_to_fetch.pop_back();
        }
        state->num_repaired++;
        state->num_failed = 0;
        LOG_DEBUG(log, "Repaired [{}/{}] parts", state->num_repaired, state->total);
    }
    catch (...)
    {
        state->num_failed++;
        if (state->num_failed == 1)
            state->retry_backoff_ms = settings->ha_unique_replay_log_retry_backoff_min_ms;
        else
            state->retry_backoff_ms = std::min(state->retry_backoff_ms * 2, settings->ha_unique_replay_log_retry_backoff_max_ms.value);
        LOG_ERROR(log, "Failed to repair part {}, num_failed={}: {}", part_name, state->num_failed, getCurrentExceptionMessage(false));
    }

    /// if we can't repair a single part for a long time, maybe the data is permanently lost (perhaps due to force leader failover).
    /// in that case, try to sync with the new leader.
    if (state->num_failed >= 60)
    {
        setUniqueTableState(UniqueTableState::REPAIR_MANIFEST);
        restarting_thread.wakeup();
        return;
    }

    if (state->delete_files_to_fetch.empty() && state->parts_to_fetch.empty())
    {
        LOG_INFO(log, "All parts have been fetched, try to repair table");
        try
        {
            auto lock = lockParts();
            resetUniqueTableToVersion(lock, state->version);
            setUniqueTableState(UniqueTableState::REPAIRED);
            auto duration = time(nullptr) - state->start_time;
            LOG_INFO(log, "Successfully repair table in {} seconds", duration);
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to repair table: {}", getCurrentExceptionMessage(false));
            setUniqueTableState(UniqueTableState::REPAIR_MANIFEST);
        }
        restarting_thread.wakeup();
    }

    if (getUniqueTableState() == UniqueTableState::REPAIR_DATA)
    {
        if (state->num_failed > 0)
            repair_data_task->scheduleAfter(state->retry_backoff_ms);
        else
            repair_data_task->schedule();
    }
}

void StorageHaUniqueMergeTree::dropPart(const String & part_name, bool detach, ContextPtr query_context)
{
    throw Exception("not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void StorageHaUniqueMergeTree::dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context, const ASTPtr & query)
{
    assertNotReadonly();
    auto zookeeper = getZooKeeper();

    if (detach && !is_leader)
    {
        // Unifies the detach and attach behavior that only leader can execute these operation.
        throw Exception("Only leader can detach partition", ErrorCodes::READONLY);
    }

    if (!is_leader)
    {
        if (sendRequestToLeaderReplica(query, query_context))
            return;
    }
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = merger_mutator.merges_blocker.cancel();

        String partition_id = getPartitionIDFromQuery(partition, query_context);

        /// For cases where some merge threads are concurrent running, cancel all the running merge states such that
        /// those threads will be aborted.
        auto write_lock = uniqueWriteLock();

        for (const auto & it : running_merge_states)
        {
            /// Only cancel those parts that belong to this partition
            if (it.first->info.partition_id == partition_id)
            {
                it.second->cancel();
            }
        }
        auto parts_to_remove = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
        if (detach)
        {
            LOG_INFO(log, "Deatch partition deatching: {} parts inside partition ID {}.", parts_to_remove.size(), partition_id);
            leaderDeatchParts(zookeeper, parts_to_remove, write_lock);
        }
        else
        {
            LOG_INFO(log, "Drop partition removing: {} parts inside partition ID {}.", parts_to_remove.size(), partition_id);
            leaderDropParts(zookeeper, parts_to_remove, write_lock);
        }
    }
}

void StorageHaUniqueMergeTree::dropPartitionWhere(const ASTPtr & predicate, bool detach, ContextPtr query_context, const ASTPtr & query)
{
    assertNotReadonly();
    auto zookeeper = getZooKeeper();

    if (!is_leader)
    {
        sendRequestToLeaderReplica(query, query_context);
        return;
    }
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = merger_mutator.merges_blocker.cancel();

        MergeTreeData::DataPartsVector parts_to_remove = getPartsByPredicate(predicate);
        if (!parts_to_remove.empty())
        {
            String partition_id = parts_to_remove[0]->info.partition_id;

            /// For cases where some merge threads are concurrent running, cancel all the running merge states such that
            /// those threads will be aborted.
            auto write_lock = uniqueWriteLock();
            for (const auto & it : running_merge_states)
            {
                /// Only cancel those parts that belong to this partition
                if (it.first->info.partition_id == partition_id)
                {
                    it.second->cancel();
                }
            }
            leaderDropParts(zookeeper, parts_to_remove, write_lock);
        }
    }
}

void  StorageHaUniqueMergeTree::truncate(const ASTPtr & query, const StorageMetadataPtr &, ContextPtr query_context, TableExclusiveLockHolder &)
{
    auto zookeeper = getZooKeeper();
    if (!is_leader)
    {
        sendRequestToLeaderReplica(query, query_context);
        return;
    }
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = merger_mutator.merges_blocker.cancel();

        /// For cases where some merge thredds are concurrent running, cancel all the running merge states such that
        /// those threads will be aborted.
        auto write_lock = uniqueWriteLock();
        for (const auto & it : running_merge_states)
        {
            it.second->cancel();
        }

        auto parts_to_remove = getDataPartsVector();
        leaderDropParts(zookeeper, parts_to_remove, write_lock);
        LOG_INFO(log, "Table truncate removing {} parts.", parts_to_remove.size());
    }
}

PartitionCommandsResultInfo StorageHaUniqueMergeTree::attachPartition(
    const ASTPtr & partition, const StorageMetadataPtr & /* metadata_snapshot */,
    bool attach_part, ContextPtr local_context)
{
    if (!is_leader)
        throw Exception("Only leader can attach partition", ErrorCodes::READONLY);

    String source_dir = "detached/";

    PartitionCommandsResultInfo results;
    PartsTemporaryRename renamed_parts(*this, "detached/");
    MutableDataPartsVector loaded_parts = tryLoadPartsToAttach(partition, attach_part, local_context, renamed_parts);

    auto zookeeper = getZooKeeper();
    {
        auto write_lock = uniqueWriteLock();

        String partition_id = loaded_parts.back()->info.partition_id;
        auto existing_parts = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

        if (!existing_parts.empty())
        {
            /// In order to keep uniqueness, no parts should existed before.
            throw Exception(
                "Partition " + partition_id + " already exists, please drop first before attaching.", ErrorCodes::LOGICAL_ERROR);
        }
    }

    // auto dict_locks = getDictionaryLocks();

    for (size_t i = 0; i < loaded_parts.size(); ++i)
    {
        Stopwatch watch;
        // if (bitengine_dictionary_manager)
        //     bitengine_dictionary_manager->checkBitEnginePart(part);

        // Allocate a new block number
        auto & part = loaded_parts[i];
        UInt64 block_number = allocateBlockNumberDirect(zookeeper);
        part->info.min_block = block_number;
        part->info.max_block = block_number;
        part->info.level = 0;
        part->name = part->getNewName(part->info);

        {
            auto manifest_lock = manifest_store->writeLock();

            Coordination::Requests ops;
            auto [lsn, set_lsn_request] = allocLSNAndMakeSetRequest(zookeeper);
            ops.emplace_back(std::move(set_lsn_request));

            // Rename and reload delete bitmap
            part->renameDeleteFileToVersion(lsn);
            auto delete_bitmap = part->readDeleteFileWithVersion(lsn, /*log_on_error*/ true);

            MergeTreeData::DataPartsDeleteSnapshot new_deletes;
            new_deletes.insert({part, delete_bitmap});

            MergeTreeData::SyncPartsTransaction local_txn(*this, new_deletes, lsn);
            local_txn.preCommit(part);

            Coordination::Responses responses;
            auto multi_code = zookeeper->tryMultiNoThrow(ops, responses);
            if (multi_code != Coordination::Error::ZOK)
            {
                throw Exception("Unexpected ZK error while adding block for LSN" + toString(lsn), ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR);
            }

            ManifestLogEntry entry;
            entry.type = ManifestLogEntry::SYNC_PARTS;
            entry.version = lsn;
            entry.prev_version = manifest_store->latestVersion();
            entry.source_replica = replica_name;
            entry.added_parts.push_back(part->name);
            try
            {
                manifest_store->deletePartRemovedVersion({part->name});
                manifest_store->append(manifest_lock, entry, /*commit=*/true);
                local_txn.commit();
                LOG_DEBUG(log, "Attach partition commited part {} at version {} in {} ms", part->name, lsn, watch.elapsedMilliseconds());

                increment.value = std::max(Int64(increment.value.load()), part->info.min_block);
                renamed_parts.old_and_new_names[i].first.clear();

                results.push_back(PartitionCommandResultInfo{
                    .partition_id = part->info.partition_id,
                    .part_name = part->name,
                    .old_part_name = loaded_parts[i]->name,
                });

            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                local_txn.rollback();
                throw;
            }
        }

        // Recode part if there exist columns with compression flag is true
        // addPartForRecode(part);

        // if (query_context.getSettingsRef().enable_async_build_bitmap_in_attach)
        //     addPartForBitMapIndex(part);

        // if (!settings->only_use_ttl_of_metadata)
        //     addPartForModifyTTL(part);

        LOG_INFO(log, "Finished attaching part {}", part->name);
    }

    local_context->dropCaches();
    return results;
}

void StorageHaUniqueMergeTree::movePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, ContextPtr query_context)
{
    auto * merge_tree_table = dynamic_cast<MergeTreeData *>(source_table.get());
    if (!merge_tree_table)
        throw Exception("Target table must be MergeTree family for Move Partition.", ErrorCodes::LOGICAL_ERROR);

    if (merge_tree_table->merging_params.mode != MergeTreeData::MergingParams::Unique)
    {
        throw Exception(
            "Move partition source table type does not match, it also have to be unique engine table.", ErrorCodes::LOGICAL_ERROR);
    }

    if (source_table->getStoragePolicy()->getName() != getStoragePolicy()->getName())
        throw Exception("Table must have same storage_policy for Move partition", ErrorCodes::LOGICAL_ERROR);

    String partition_id = getPartitionIDFromQuery(partition, query_context);
    std::vector<std::pair<String, String>> parts_to_move;

    /// copy all the list in fromtbl's detached directory to table's detached directory
    for (const auto & disk : merge_tree_table->getStoragePolicy()->getDisks())
    {
        String from_path = merge_tree_table->getFullPathOnDisk(disk) + "detached/";
        String to_path = getFullPathOnDisk(disk) + "detached/";
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(from_path); it != Poco::DirectoryIterator(); ++it)
        {
            const String & name = it.name();
            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(name, &part_info, format_version) || part_info.partition_id != partition_id)
                continue;

            /// check whether partition already exist in table's detached directory, raise error to avoid data overwrite
            for (const auto & to_path_ : getDataPaths())
            {
                if (Poco::File(to_path_ + "detached/" + name).exists())
                {
                    throw Exception(
                        "Partition " + partition_id + " already exist in table " + getStorageID().table_name + " detached directory, please check!",
                        ErrorCodes::LOGICAL_ERROR);
                }
            }

            parts_to_move.emplace_back(from_path + name, to_path + name);
        }
    }

    if (parts_to_move.empty())
    {
        throw Exception(
            "No parts to move in partition: " + partition_id + " in table: " + source_table->getStorageID().table_name, ErrorCodes::LOGICAL_ERROR);
    }

    LOG_DEBUG(log, "Found {} parts of partition {} in table {}", parts_to_move.size(), partition_id, source_table->getStorageID().table_name);

    for (auto & [from_path, to_path] : parts_to_move)
    {
        Poco::File(from_path).renameTo(to_path);
        LOG_DEBUG(log, "Move part from {} to {}", from_path, to_path);
    }
}

std::unique_ptr<MergeTreeSettings> StorageHaUniqueMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getReplicatedMergeTreeSettings());
}

void StorageHaUniqueMergeTree::alter(
    const AlterCommands & commands,
    ContextPtr query_context,
    TableLockHolder & table_lock_holder,
    const ASTPtr & query)
{
    LOG_DEBUG(log, "Doing ALTER");

    if (!is_leader)
    {
        sendRequestToLeaderReplica(query, query_context);
        return;
    }

    // Verify supported commands.
    for (const auto & command : commands)
    {
        // clang-format off
        if (command.type != AlterCommand::ADD_COLUMN &&
            command.type != AlterCommand::DROP_COLUMN &&
            command.type != AlterCommand::MODIFY_TTL)
        {
            throw Exception("Not supported alter commands", ErrorCodes::CANNOT_ASSIGN_ALTER);
        }
        // clang-format on
    }

    auto ast_to_str = [](ASTPtr query_ast) -> String
    {
        if (!query_ast)
            return "";
        return queryToString(query_ast);
    };

    auto current_metadata = getInMemoryMetadataPtr();
    String old_metadata_str = TableMetadata(*this, current_metadata).toString();
    String old_columns_str = current_metadata->columns.toString();

    StorageInMemoryMetadata future_metadata = *current_metadata;
    commands.apply(future_metadata, query_context);
    checkColumnsValidity(future_metadata.columns);

    TableMetadata future_metadata_in_zk(*this, current_metadata);
    if (ast_to_str(future_metadata.sorting_key.definition_ast) != ast_to_str(current_metadata->sorting_key.definition_ast))
    {
        /// We serialize definition_ast as list, because code which apply ALTER (setTableStructure) expect serialized non empty expression
        /// list here and we cannot change this representation for compatibility. Also we have preparsed AST `sorting_key.expression_list_ast`
        /// in KeyDescription, but it contain version column for VersionedCollapsingMergeTree, which shouldn't be defined as a part of key definition AST.
        /// So the best compatible way is just to convert definition_ast to list and serialize it. In all other places key.expression_list_ast should be used.
        future_metadata_in_zk.sorting_key = serializeAST(*extractKeyExpressionList(future_metadata.sorting_key.definition_ast));
    }

    if (ast_to_str(future_metadata.sampling_key.definition_ast) != ast_to_str(current_metadata->sampling_key.definition_ast))
        future_metadata_in_zk.sampling_expression = serializeAST(*extractKeyExpressionList(future_metadata.sampling_key.definition_ast));

    if (ast_to_str(future_metadata.partition_key.definition_ast) != ast_to_str(current_metadata->partition_key.definition_ast))
        future_metadata_in_zk.partition_key = serializeAST(*extractKeyExpressionList(future_metadata.partition_key.definition_ast));

    if (ast_to_str(future_metadata.table_ttl.definition_ast) != ast_to_str(current_metadata->table_ttl.definition_ast))
    {
        if (future_metadata.table_ttl.definition_ast)
            future_metadata_in_zk.ttl_table = serializeAST(*future_metadata.table_ttl.definition_ast);
        else /// TTL was removed
            future_metadata_in_zk.ttl_table = "";
    }

    String new_indices_str = future_metadata.secondary_indices.toString();
    if (new_indices_str != current_metadata->secondary_indices.toString())
        future_metadata_in_zk.skip_indices = new_indices_str;

    String new_projections_str = future_metadata.projections.toString();
    if (new_projections_str != current_metadata->projections.toString())
        future_metadata_in_zk.projections = new_projections_str;

    String new_constraints_str = future_metadata.constraints.toString();
    if (new_constraints_str != current_metadata->constraints.toString())
        future_metadata_in_zk.constraints = new_constraints_str;

    String new_metadata_str = future_metadata_in_zk.toString();
    String new_columns_str = future_metadata.columns.toString();

    if (ast_to_str(current_metadata->settings_changes) != ast_to_str(future_metadata.settings_changes))
    {
        /// Just change settings
        auto table_id = getStorageID();
        StorageInMemoryMetadata metadata_copy = *current_metadata;
        metadata_copy.settings_changes = future_metadata.settings_changes;
        changeSettings(metadata_copy.settings_changes, table_lock_holder);
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(query_context, table_id, metadata_copy);
    }
    if (new_metadata_str == old_columns_str && new_columns_str == new_columns_str)
    {
        // Nothing chagned, returns
        return;
    }

    LOG_INFO(log, "New alter manifest log, metadata:{}", new_metadata_str);
    LOG_INFO(log, "New alter manifest log, columns: {}", new_columns_str);

    try
    {
        // Step 1: do local altering for leader node first
        alterTableSchema(new_metadata_str, new_columns_str, table_lock_holder);

        // Step 2: Add a new `MODIFY_SCHEMA` type manifest log and commit
        auto zookeeper = getZooKeeper();
        auto manifest_lock = manifest_store->writeLock();
        auto version = allocLSNAndSet(zookeeper);
        ManifestLogEntry entry;
        entry.type = ManifestLogEntry::MODIFY_SCHEMA;
        entry.version = version;
        entry.prev_version = manifest_store->latestVersion();
        entry.source_replica = replica_name;
        entry.metadata_str = new_metadata_str;
        entry.columns_str = new_columns_str;
        manifest_store->append(manifest_lock, entry, /*commit=*/true);

        /// Step 3: Wake up the alter thread to alter parts asynchronously.
        alter_thread.wakeup();
    }
    catch (...)
    {
        table_lock_holder.reset();
        verifyTableStructureAlterIfNeed(old_metadata_str, old_columns_str);
        alterTableSchema(old_metadata_str, old_columns_str, table_lock_holder);
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        throw;
    }
}

template <class Func>
void StorageHaUniqueMergeTree::foreachCommittedParts(Func && func) const
{

    auto lock = lockParts();
    for (const auto & part : getDataPartsStateRange(DataPartState::Committed))
    {
        if (part->isEmpty())
            continue;

        func(part);
    }
}

void StorageHaUniqueMergeTree::setTableStructure(
    ColumnsDescription new_columns, const ReplicatedMergeTreeTableMetadata::Diff & metadata_diff)
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

        if (metadata_diff.projections_changed)
            new_metadata.projections = ProjectionsDescription::parse(metadata_diff.new_projections, new_columns, getContext());

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

std::optional<UInt64> StorageHaUniqueMergeTree::totalRows(const Settings & settings) const
{
    UInt64 res = 0;
    foreachCommittedParts([&res](auto & part) { res += part->rows_count; });
    return res;
}

std::optional<UInt64> StorageHaUniqueMergeTree::totalBytes(const Settings & settings) const
{
    UInt64 res = 0;
    foreachCommittedParts([&res](auto & part) { res += part->getBytesOnDisk(); } );
    return res;
}
}

