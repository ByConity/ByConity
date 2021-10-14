#include <Storages/MergeTree/HaMergeTreeQueue.h>

#include <Storages/StorageHaMergeTree.h>
#include <Interpreters/MutationLog.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ASSIGN_ALTER;
}

HaMergeTreeQueue::HaMergeTreeQueue(StorageHaMergeTree & storage_)
    : storage(storage_)
    , format_version(storage.format_version)
    , zookeeper_path(storage.zookeeper_path)
    , replica_name(storage.replica_name)
    , replica_path(storage.replica_path)
    , log(&Poco::Logger::get(storage.getStorageID().getFullTableName() + " (HaMergeTreeQueue)"))
    , unprocessed_queue(unprocessed_set.get<TagSequenced>())
    , unprocessed_by_lsn(unprocessed_set.get<TagByLSN>())
    , merge_mutate_future_parts(storage.format_version)
{
}


void HaMergeTreeQueue::initialize(const MergeTreeData::DataParts &)
{
    /// empty
}

void HaMergeTreeQueue::clear()
{
    std::lock_guard clear_lock(state_mutex);
    unprocessed_queue.clear();
    merge_mutate_future_parts.clear();
    for (auto & elem : unprocessed_counts)
        elem.store(0, std::memory_order_relaxed);
    unprocessed_merges_of_self.store(0, std::memory_order_relaxed);
    unprocessed_mutations_of_self.store(0, std::memory_order_relaxed);
    unprocessed_inserts_by_time.clear();
    min_unprocessed_insert_time.store(std::numeric_limits<time_t>::max(), std::memory_order_relaxed);
    max_processed_insert_time.store(0, std::memory_order_relaxed);
    last_queue_update_time.store(0, std::memory_order_relaxed);
}

HaMergeTreeLogManager & HaMergeTreeQueue::getLogManager() const
{
    return *storage.log_manager;
}

HaMergeTreeLogExchanger & HaMergeTreeQueue::getLogExchanger() const
{
    return storage.log_exchanger;
}

zkutil::ZooKeeperPtr HaMergeTreeQueue::getZooKeeper() const
{
    return storage.getZooKeeper();
}

void HaMergeTreeQueue::load(zkutil::ZooKeeperPtr zookeeper)
{
    int inserted_count = 0;

    {
        std::lock_guard insert_lock(state_mutex);
        auto manager_lock = getLogManager().lockMe();
        for (auto & entry : getLogManager().getEntryListUnsafe())
            inserted_count += insertWithStats(entry, insert_lock);
    }

    if (inserted_count)
        LOG_TRACE(log, "Copy {} entries into queue from log manager", inserted_count);

    {
        std::lock_guard pull_logs_lock(pull_logs_to_queue_mutex);
        zookeeper->tryGet(replica_path + "/mutation_pointer", mutation_pointer);
    }
}

/**
 * Only skip clone logs with the current time is between utc_time_to_stop_clone and utc_time_to_start_clone.
 * utc_time_interval_allow_clone / utc_time_interval_stop_clone is a string consists of a list of UTC time.
 * we will parse it into a list of time intervals, every two int elements consist an interval.
 * utc_time_interval_allow_clone has the highest priority
 * utc_time_interval_stop_clone is the second prioiry and utc_time_to_stop_clone is the lowest priority.
 *
 * for example utc_time_interval_allow_clone = "1,2,3,4,5,6" will be parsed to (1,2), (3,4), (5,6)
 * */
bool HaMergeTreeQueue::canExecuteClone(const HaMergeTreeLogEntryPtr & clone_entry)
{
    if (!clone_entry || clone_entry->type != LogEntry::CLONE_PART)
        return true;

    /* TODO:
    auto & settings = storage.global_context.getSettingsRef();

    if (!settings.stop_clone_in_utc_time)
        return true;

    const DateLUTImpl & date_lut = DateLUT::instance("UTC");
    time_t now_time = time(nullptr);
    UInt64 now_hour = date_lut.toHour(now_time);

    auto allow_time_interval = getTimeInterval(settings.utc_time_interval_allow_clone);
    auto stop_time_interval = getTimeInterval(settings.utc_time_interval_stop_clone);

    for (auto it = allow_time_interval.begin(); it != allow_time_interval.end(); ++it)
    {
        if (now_hour >= it->first && now_hour <= it->second)
        {
            return true;
        }
    }

    for (auto it = stop_time_interval.begin(); it != stop_time_interval.end(); ++it)
    {
        if (now_hour >= it->first && now_hour <= it->second)
        {
            return false;
        }
    }

    if (now_hour >= settings.utc_time_to_stop_clone && now_hour < settings.utc_time_to_start_clone)
    {
        return false;
    }


    */
    return true;
}


HaMergeTreeQueue::LSNList HaMergeTreeQueue::prepareRequestLSNs(zkutil::ZooKeeperPtr zookeeper, bool ignore_backoff)
{
    auto updated_lsn = getLogManager().getLSNStatus(true).updated_lsn;
    UInt64 latest_lsn = parse<UInt64>(zookeeper->get(storage.getZKLatestLSNPath()));

    /// Ideally, there should not be LSNs which less then `updated_lsn`.
    /// But in order to tolerate potential bug or other incorrect state, remove those stale LSNs.
    while (prepared_lsn_list.begin() != prepared_lsn_list.end() && prepared_lsn_list.begin()->first <= updated_lsn)
        prepared_lsn_list.erase(prepared_lsn_list.begin());

    {
        auto manager_lock = getLogManager().lockMe();
        auto & manager_entries = getLogManager().getEntryListUnsafe();

        /// Remove fetched lsns
        for (auto prepared_it = prepared_lsn_list.begin(); prepared_it != prepared_lsn_list.end();)
        {
            if (manager_entries.count(prepared_it->first))
                prepared_it = prepared_lsn_list.erase(prepared_it);
            else
                ++prepared_it;
        }

        /// find the first un-fetched lsn
        next_prepared_lsn = std::max(next_prepared_lsn, updated_lsn + 1);
        auto it = manager_entries.lower_bound(next_prepared_lsn);
        while (it != manager_entries.end() && (*it)->lsn == next_prepared_lsn)
        {
            ++next_prepared_lsn; /// increase next_prepared_lsn only if the log is stored in log manager
            ++it;
        }

        /// Prepare new lsns
        for (UInt64 lsn = next_prepared_lsn; lsn <= latest_lsn; ++lsn)
        {
            if (!manager_entries.count(lsn))
                prepared_lsn_list.try_emplace(lsn);
        }
    }

    /// Prepare list for request
    time_t now = time(nullptr);

    LSNList lsns;
    for (auto & [lsn, stat] : prepared_lsn_list)
    {
        /// exp backoff
        if (ignore_backoff || now - stat.last_attempt_time > (1ll << std::min(stat.num_tries, 10u)))
        {
            if (0 == stat.first_attempt_time)
                stat.first_attempt_time = now;
            stat.last_attempt_time = now;
            if (!ignore_backoff)
                stat.num_tries += 1;
            lsns.push_back(lsn);
        }
    }

    return lsns;
}

/// Assume sorted
void HaMergeTreeQueue::onFetchedEntries(const LSNList & requests, const LogEntryVec & fetched, bool all_success)
{
    for (auto & e : fetched)
        prepared_lsn_list.erase(e->lsn);

    if (auto failed_lsns = logSetDifference(requests, fetched); !failed_lsns.empty())
        processFailedLSNs(failed_lsns, all_success);
}

void HaMergeTreeQueue::processFailedLSNs(const LSNList & failed_lsns, bool all_success)
{
    auto curr_time = time(nullptr);
    auto storage_settings = storage.getSettings();

    std::ostringstream oss;

    LogEntryVec bad_entries;

    for (auto lsn : failed_lsns)
    {
        auto it = prepared_lsn_list.find(lsn);
        /// Logical error, maybe logging here...
        if (it == prepared_lsn_list.end())
            continue;
        auto & stat = it->second;

        /// TODO: remove magic number
        /// 1. exceed timeout
        /// 2. nobody know the lsn
        UInt64 stale_time = static_cast<UInt64>(curr_time - stat.first_attempt_time);
        if (stale_time > storage_settings->ha_mark_bad_lsn_timeout
            || (all_success && stale_time > storage_settings->ha_mark_bad_lsn_min_timeout
                && stat.num_tries > storage_settings->ha_mark_bad_lsn_min_timeout / (1 << 10u)))
        {
            LOG_WARNING(log, "Tried to fetch LSN {} for {} times, Mark it BAD", lsn, stat.num_tries);

            auto entry = std::make_shared<LogEntry>();
            entry->type = LogEntry::BAD_LOG;
            entry->create_time = curr_time;
            entry->lsn = lsn;
            entry->source_replica = storage.replica_name;
            entry->is_executed = true;

            bad_entries.push_back(std::move(entry));
            prepared_lsn_list.erase(it);
        }
        else
        {
            oss << " | LSN " << lsn << ",FAIL " << stat.num_tries;
        }
    }

    if (!bad_entries.empty())
        write(bad_entries);

    if (log->trace())
        LOG_TRACE(log, "Failed to fetch LSN: {} ", oss.str());
    else if (log->debug())
    {
        LOG_DEBUG(log, "Failed to fetch LSN: {}, range [{}, {}]", failed_lsns.size(), failed_lsns.front(), failed_lsns.back());
    }
}

/** TODO: The is potential problem in pullLogsToQueue():
 *  When a replica (A) executes INSERT, it allocates a LSN from ZooKeeper first, then write the GET log into disk, and
 *  calls LogManager::putLogEntries() at last.
 *
 *  But another replica (B) may call Queue::pullLogsQueue at once while ZooKeeper generates a LSN, then
 *  calls LogManager::fetchLogEntries(). It means that B may fetch nothing due to the GET log generated by A
 *  not been written to disk yet.
 */
HaMergeTreeQueue::LogEntryVec HaMergeTreeQueue::pullLogsImpl(
    zkutil::ZooKeeperPtr & zookeeper,
    [[maybe_unused]] std::lock_guard<std::mutex> & pull_logs_to_queue_lock,
    Coordination::WatchCallback watch_callback,
    bool ignore_backoff)
{
    Stopwatch watch;
    SCOPE_EXIT({
        if (auto elapsed = watch.elapsedMilliseconds(); elapsed > 2500)
            LOG_DEBUG(log, "pullLogsImpl elapsed {} ms. ", elapsed);
    });

    /// should propagate exception to the caller if failed to access ZK
    auto request_lsns = prepareRequestLSNs(zookeeper, ignore_backoff);

    /// Try to pull logs from other replicas
    LogEntryVec fetch_entries;
    try
    {
        if (request_lsns.empty())
        {
            /// We still ping peer replicas to ensure them alive
            getLogExchanger().ping();
            last_queue_update_time.store(time(nullptr), std::memory_order_relaxed);
        }
        else
        {
            LOG_DEBUG(log, "Try to fetch {} entries, range: [{}, {}].", request_lsns.size(), request_lsns.front(), request_lsns.back());

            bool all_success = false;
            fetch_entries = getLogExchanger().fetchLogEntries(request_lsns, all_success);
            onFetchedEntries(request_lsns, fetch_entries, all_success);
            last_queue_update_time.store(time(nullptr), std::memory_order_relaxed);
        }
    }
    catch (const Exception &)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    /// Note that MergePredicate requires a successful execution of `updateMutations` in order to function,
    /// because otherwise it can't determine the correct current mutation version of its input part.
    /// Therefore exception should be thrown if we failed to update mutations.
    {
        std::lock_guard update_mutations_lock(update_mutations_mutex);
        updateMutations(zookeeper, update_mutations_lock, watch_callback);
    }

    return fetch_entries;
}

HaMergeTreeQueue::LogEntryVec HaMergeTreeQueue::pullLogs(zkutil::ZooKeeperPtr zookeeper)
{
    std::lock_guard lock(pull_logs_to_queue_mutex);
    return pullLogsImpl(zookeeper, lock, {});
}

bool HaMergeTreeQueue::pullLogsToQueue(
    [[maybe_unused]] zkutil::ZooKeeperPtr zookeeper,
    [[maybe_unused]] Coordination::WatchCallback watch_callback,
    [[maybe_unused]] bool ignore_backoff)
{
    std::lock_guard lock(pull_logs_to_queue_mutex);
    /// DO NOT broadcast
    write(pullLogsImpl(zookeeper, lock, watch_callback, ignore_backoff), false);
    return prepared_lsn_list.empty();
}

namespace
{
ActiveDataPartSet getCommittedPartsToMutate(StorageHaMergeTree & storage, Int64 mutation_version, const NameOrderedSet & partition_ids)
{
    MergeTreeData::DataPartsVector committed_parts;
    if (partition_ids.empty())
        committed_parts = storage.getDataPartsVector();
    else
    {
        for (auto partition_id : partition_ids)
        {
            auto data_parts = storage.getDataPartsVectorInPartition(MergeTreeData::DataPartState::Committed, partition_id);
            committed_parts.insert(committed_parts.end(), data_parts.begin(), data_parts.end());
        }
    }

    ActiveDataPartSet res(storage.format_version);
    for (auto & part : committed_parts)
    {
        if (part->info.getDataVersion() < mutation_version)
            res.add(part->name);
    }
    return res;
}
}

void HaMergeTreeQueue::updateMutations(
    zkutil::ZooKeeperPtr & zookeeper,
    [[maybe_unused]] std::lock_guard<std::mutex> & update_mutations_lock,
    Coordination::WatchCallback watch_callback)
{
    Strings entries_in_zk = zookeeper->getChildrenWatch(zookeeper_path + "/mutations", nullptr, watch_callback);
    std::set<String> entries_in_zk_set(entries_in_zk.begin(), entries_in_zk.end());

    /// Compare with the local state, delete obsolete entries and determine which new entries to load.
    Strings entries_to_load;
    {
        std::lock_guard state_lock(state_mutex);

        for (auto it = mutations_by_znode.begin(); it != mutations_by_znode.end();)
        {
            const HaMergeTreeMutationEntry & entry = *it->second.entry;
            if (!entries_in_zk_set.count(entry.znode_name))
            {
                if (!it->second.is_done)
                {
                    LOG_INFO(log, "Removing killed mutation " + entry.getNameForLogs() + " from local state.");
                    if (entry.isAlterMutation())
                    {
                        LOG_DEBUG(log, "Finishing " + entry.getNameForLogs() + " : mutation is killed");
                        alter_sequence.finishDataAlter(entry.alter_version, state_lock);
                    }
                    storage.writeMutationLog(MutationLogElement::MUTATION_ABORT, entry);
                }
                else
                    LOG_INFO(log, "Removing obsolete mutation " + entry.getNameForLogs() + " from local state.");

                mutations_by_version.erase(entry.block_number);
                it = mutations_by_znode.erase(it);
            }
            else
                ++it;
        }

        for (auto it = mutations_for_alter_metadata.begin(); it != mutations_for_alter_metadata.end();)
        {
            const HaMergeTreeMutationEntry & entry = *it->second.entry;

            if (!entries_in_zk_set.count(entry.znode_name))
            {
                /// NOTE: Do not need to remove killed or obsolete ALTER_METADATA entries from local state
                ///       due to they cannot be killed or cancelled
                if (!it->second.is_done)
                    LOG_WARNING(
                        log, "Found an unfinished mutation " + entry.getNameForLogs() + " which has no corresponding znode.");
                else
                    LOG_INFO(log, "Removing obsolete mutation " + entry.getNameForLogs() + " from local state.");

                it = mutations_for_alter_metadata.erase(it);
            }
            else
                ++it;
        }

        for (const String & znode : entries_in_zk_set)
        {
            if (!mutations_by_znode.count(znode) && !mutations_for_alter_metadata.count(znode))
                entries_to_load.push_back(znode);
        }
    }

    bool some_mutations_are_probably_done = false;

    if (!entries_to_load.empty())
    {
        LOG_INFO(log, "Loading {} mutation entries: {} - {}", entries_to_load.size(), entries_to_load.front(), entries_to_load.back());

        std::vector<std::future<Coordination::GetResponse>> futures;
        for (const String & entry : entries_to_load)
            futures.emplace_back(zookeeper->asyncGet(zookeeper_path + "/mutations/" + entry));

        std::vector<HaMergeTreeMutationEntryPtr> new_mutations;
        for (size_t i = 0; i < entries_to_load.size(); ++i)
        {
            new_mutations.push_back(std::make_shared<HaMergeTreeMutationEntry>(
                HaMergeTreeMutationEntry::parse(futures[i].get().data, entries_to_load[i])));
        }

        bool new_data_mutations = false;
        bool new_alter_metadata = false;
        /// loading mutations into local state
        {
            std::lock_guard state_lock(state_mutex);
            for (const HaMergeTreeMutationEntryPtr & entry : new_mutations)
            {
                if (entry->isAlterMetadata())
                {
                    auto & mutation = mutations_for_alter_metadata.emplace(entry->znode_name, MutationStatus(entry, format_version)).first->second;
                    if (entry->znode_name <= mutation_pointer)
                    {
                        mutation.is_done = true;
                        mutation.finish_time = time(nullptr);
                    }
                    else
                    {
                        LOG_INFO(log, "Adding alter metadata mutation {} with version {}", entry->getNameForLogs(), entry->alter_version);

                        alter_sequence.addMetadataAlter(entry->alter_version, entry->commands, state_lock);
                        new_alter_metadata = true;
                    }
                }
                else
                {
                    auto & mutation = mutations_by_znode.emplace(entry->znode_name, MutationStatus(entry, format_version))
                        .first->second;
                    mutations_by_version.emplace(entry->block_number, &mutation);
                    if (entry->znode_name <= mutation_pointer)
                    {
                        mutation.is_done = true;
                        mutation.finish_time = time(nullptr);
                    }
                    else
                    {
                        /// Initialize `mutation.parts_to_do` from local committed parts
                        mutation.parts_to_do = getCommittedPartsToMutate(storage, entry->block_number, entry->partition_ids);
                        LOG_INFO(log, "Adding mutation {} with {} parts to mutate", entry->getNameForLogs(), mutation.parts_to_do.size());
                        if (mutation.parts_to_do.size() == 0)
                            some_mutations_are_probably_done = true;

                        if (entry->isAlterMutation())
                        {
                            LOG_DEBUG(log, "Adding " + entry->getNameForLogs());
                            alter_sequence.addMutationForAlter(entry->alter_version, entry->commands, state_lock);
                        }
                        new_data_mutations = true;
                    }
                }

                if (max_loaded_mutation_version.load(std::memory_order_relaxed) < entry->block_number)
                    max_loaded_mutation_version.store(entry->block_number, std::memory_order_relaxed);
            } /// end for
        } /// end state lock

        if (new_data_mutations)
            storage.merge_selecting_task->schedule();
        if (new_alter_metadata)
            storage.alter_thread.start();
    }

    if (some_mutations_are_probably_done)
        storage.mutations_finalizing_task->schedule();
}

HaMergeTreeMutationEntryPtr HaMergeTreeQueue::removeMutation(zkutil::ZooKeeperPtr zookeeper, const String & mutation_id)
{
    std::lock_guard lock(update_mutations_mutex);

    auto rc = zookeeper->tryRemove(zookeeper_path + "/mutations/" + mutation_id);
    if (rc == Coordination::Error::ZOK)
        LOG_DEBUG(log, "Removed mutation " + mutation_id + " from ZooKeeper.");

    HaMergeTreeMutationEntryPtr entry;
    bool mutation_was_active = false;
    {
        std::lock_guard state_lock(state_mutex);

        auto it = mutations_by_znode.find(mutation_id);
        if (it == mutations_by_znode.end())
            return nullptr;

        mutation_was_active = !it->second.is_done;
        entry = it->second.entry;

        LOG_INFO(log, "Removing killed mutation " + entry->getNameForLogs() + " from local state.");
        if (entry->isAlterMutation())
        {
            LOG_DEBUG(log, "Finishing " + entry->getNameForLogs() + " : mutation is killed");
            alter_sequence.finishDataAlter(entry->alter_version, state_lock);
        }
        storage.writeMutationLog(MutationLogElement::MUTATION_ABORT, *entry);

        mutations_by_version.erase(entry->block_number);
        mutations_by_znode.erase(it);
        LOG_INFO(log, "Removed mutation " + entry->znode_name + " from local state.");
    }

    if (mutation_was_active)
        storage.background_executor.triggerTask();

    return entry;
}

/// used by "system reload mutation" command
void HaMergeTreeQueue::forceReloadMutation(const String & mutation_id)
{
    bool some_mutations_are_probably_done = false;
    {
        std::lock_guard state_lock(state_mutex);
        auto it = mutations_by_znode.find(mutation_id);
        if (it != mutations_by_znode.end())
        {
            auto & mutation = it->second;
            auto & entry = mutation.entry;
            auto old_num_parts = mutation.parts_to_do.size();
            mutation.parts_to_do = getCommittedPartsToMutate(storage, entry->block_number, entry->partition_ids);
            LOG_INFO(log, "Recalculated parts_to_do for mutation {}: from {} to {}", entry->getNameForLogs(), old_num_parts, mutation.parts_to_do.size());
            if (mutation.parts_to_do.size() == 0)
                some_mutations_are_probably_done = true;
        }
    }
    if (some_mutations_are_probably_done)
        storage.mutations_finalizing_task->schedule();
}

void HaMergeTreeQueue::removeCoveredPartsFromMutation(const LogEntry & entry, std::lock_guard<std::mutex> & /*states_lock*/)
{
    String part_name;
    if (entry.type == LogEntry::DROP_RANGE || entry.type == LogEntry::CLEAR_RANGE)
        part_name = entry.new_parts.front();
    else if (!entry.actual_committed_part.empty())
        part_name = entry.actual_committed_part;
    else
        return;

    auto part_info = MergeTreePartInfo::fromPartName(part_name, storage.format_version);
    if (part_info.level == MergeTreePartInfo::MAX_LEVEL && part_info.mutation == 0)
    {
        /// fix mutation field for drop/clear range log created before adding mutation support
        part_info.mutation = MergeTreePartInfo::MAX_MUTATION;
    }

    auto data_version = part_info.getDataVersion();
    bool some_mutations_are_probably_done = false;
    for (auto & mutation_entry : mutations_by_version)
    {
        auto mutation_version = mutation_entry.first;
        MutationStatus & status = *mutation_entry.second;
        if (status.is_done || !status.entry->coverPartitionId(part_info.partition_id))
            continue;

        if (data_version < mutation_version)
        {
            status.parts_to_do.add(part_name); /// add will also remove covered parts
        }
        else if (part_info.min_block < mutation_version)
        {
            /// note that drop/clear range will always go to this branch
            status.parts_to_do.removePartsCoveredBy(part_name);
            if (status.parts_to_do.size() == 0)
                some_mutations_are_probably_done = true;
        }

        if (!status.latest_failed_part.empty() && part_info.contains(status.latest_failed_part_info))
        {
            status.latest_failed_part.clear();
            status.latest_failed_part_info = MergeTreePartInfo();
            status.latest_fail_time = 0;
            status.latest_fail_reason.clear();
        }
    }

    if (some_mutations_are_probably_done)
        storage.mutations_finalizing_task->schedule();
}

size_t HaMergeTreeQueue::markRedundantEntries(LogEntryVec & entries)
{
    size_t marked_count = 0;

    /// helper structures
    ActiveDataPartSet virtual_parts(storage.format_version);
    std::unordered_map<String, Int64> min_block_map;

    /// helper functions
    auto is_dropped = [&](const LogEntry & entry) {
        for (auto & new_part : entry.new_parts)
        {
            auto part_info = MergeTreePartInfo::fromPartName(new_part, storage.format_version);
            auto iter = min_block_map.find(part_info.partition_id);
            /// No record
            if (iter == min_block_map.end())
                return false;
            /// Not dropped
            if (part_info.min_block > iter->second)
                return false;
        }
        return true;
    };
    auto is_covered = [&](const LogEntry & entry) {
        for (auto & new_part : entry.new_parts)
        {
            if (virtual_parts.getContainingPart(new_part).empty())
                return false;
        }
        return true;
    };

    // std::sort(entries.begin(), entries.end(), [](const auto & l, const auto & r) { return l->lsn < r->lsn; });

    /// Reverse travesal
    for (auto iter = entries.rbegin(); iter != entries.rend(); ++iter)
    {
        auto & entry = *iter;
        LOG_TRACE(log, "check entry: {}", entry->toString());

        if (entry->type == LogEntry::BAD_LOG)
        {
            entry->is_executed = true;
        }
        else if (entry->new_parts.empty())
        {
            entry->is_executed = false;
        }
        else if (entry->type == LogEntry::DROP_RANGE)
        {
            /// Update min_block for DROP_RANGE
            auto part_info = MergeTreePartInfo::fromPartName(entry->new_parts.front(), storage.format_version);

            if (min_block_map.find(part_info.partition_id) == min_block_map.end())
            {
                /// Earliest DROP_RANGE in this partition
                min_block_map[part_info.partition_id] = part_info.max_block;
                entry->is_executed = false;
            }
            else
            {
                /// Later DROP_RANGE must be covered
                entry->is_executed = true;
            }
        }
        else if (is_dropped(*entry) || is_covered(*entry))
        {
            /// If new part is covered, covered part must be of CLONE_PART due to reverse traversal.
            entry->is_executed = true;
        }
        else
        {
            if (entry->type == LogEntry::GET_PART || entry->type == LogEntry::CLONE_PART)
            {
                /// DO NOT add MERGE_PARTS
                LOG_TRACE(log, "Add virtual parts: {}" , entry->toString());
                virtual_parts.add(entry->new_parts.front());
            }

            entry->is_executed = false;
        }

        marked_count += size_t(entry->is_executed);
    }

    return marked_count;
}

size_t HaMergeTreeQueue::insertWithStats(const LogEntryPtr & entry, std::lock_guard<std::mutex> &)
{
    if (entry->is_executed)
    {
        if (entry->type == LogEntry::GET_PART)
            max_processed_insert_time = std::max<time_t>(entry->create_time, max_processed_insert_time);
    }
    else
    {
        if (entry->type == LogEntry::GET_PART || entry->type == LogEntry::REPLACE_PARTITION)
        {
            if (entry->create_time < min_unprocessed_insert_time.load(std::memory_order_relaxed))
                min_unprocessed_insert_time.store(entry->create_time);
            unprocessed_inserts_by_time.insert(entry);
        }

        if (0 == unprocessed_by_lsn.count(entry->lsn))
        {
            if (entry->type == LogEntry::DROP_RANGE || entry->type == LogEntry::CLEAR_RANGE
                || entry->type == LogEntry::REPLACE_PARTITION) /// pick DROP_RANGE first
                unprocessed_queue.push_front(entry);
            else
                unprocessed_queue.push_back(entry);

            if (entry->hasMergeMutateFutureParts())
                merge_mutate_future_parts.add(entry->lsn, entry->new_parts);

            unprocessed_counts[entry->type].fetch_add(1, std::memory_order_relaxed);
            if (entry->type == LogEntry::MERGE_PARTS && entry->source_replica == storage.replica_name)
                unprocessed_merges_of_self.fetch_add(1, std::memory_order_relaxed);
            if (entry->type == LogEntry::MUTATE_PART && entry->source_replica == storage.replica_name)
                unprocessed_mutations_of_self.fetch_add(1, std::memory_order_relaxed);

            LOG_TRACE(log, "Insert entry to queue: {} ", entry->toString());
            return 1;
        }
    }

    return 0;
}

void HaMergeTreeQueue::write(const LogEntryVec & entries, bool broadcast)
{
    if (entries.empty())
        return;

    /// Mark trivial entries ahead for convenience and performance
    for (auto & entry : entries)
    {
        if (entry->shouldSkipOnReplica(storage.replica_name))
            entry->is_executed = true;
    }

    auto written_entries = getLogManager().writeLogEntries(entries);

    if (broadcast)
    {
        getLogExchanger().putLogEntries(written_entries);
    }

    size_t num_new_unprocessed = 0;
    {
        std::lock_guard insert_lock(state_mutex);
        for (auto & entry : entries)
            num_new_unprocessed += insertWithStats(entry, insert_lock);
    }

    if (num_new_unprocessed > 0)
        storage.background_executor.triggerTask();
}

HaQueueExecutingEntrySetPtr HaMergeTreeQueue::tryCreateExecutingSet(const LogEntryPtr & e)
{
    std::lock_guard state_lock(state_mutex);
    if (e->currently_executing)
        return nullptr;

    auto res = std::make_shared<HaQueueExecutingEntrySet>(*this);
    res->setExecuting(e);
    return res;
}

HaQueueExecutingEntrySetPtr HaMergeTreeQueue::selectEntryToProcess(size_t & num_avail_fetches)
{
    HaQueueExecutingEntrySetPtr res;
    LogEntry::Vec useless_entries;
    {
        std::lock_guard state_lock(state_mutex);
        res = selectEntryToProcessLocked(num_avail_fetches, useless_entries, state_lock);
    }
    removeProcessedEntries(useless_entries);
    return res;
}

HaQueueExecutingEntrySetPtr HaMergeTreeQueue::selectEntryToProcessLocked(
    size_t & num_avail_fetches, LogEntry::Vec & useless_entries, std::lock_guard<std::mutex> & state_lock)
{
    auto storage_settings = storage.getSettings();
    auto res = std::make_shared<HaQueueExecutingEntrySet>(*this);

    /// stat only
    num_avail_fetches = 0;
    for (auto & entry : unprocessed_queue)
    {
        if (!entry->currently_executing && (entry->type == LogEntry::GET_PART || entry->type == LogEntry::CLONE_PART)
            && entry->num_tries == 0 && canExecuteClone(entry))
            num_avail_fetches += 1;
    }

    auto is_useless = [&](const LogEntryPtr & e) {
        for (auto & part_name : e->new_parts)
        {
            if (!storage.getActiveContainingPart(part_name))
                return false;
        }
        return true;
    };

    auto now = time(nullptr);

    for (auto it = unprocessed_queue.begin(); it != unprocessed_queue.end(); ++it)
    {
        auto & entry = *it;
        if (entry->currently_executing)
            continue;

        if (entry->willCommitNewPart() && is_useless(entry))
        {
            useless_entries.push_back(entry);
            continue;
        }

        if (entry->isAlterMutation())
        {
            if (!alter_sequence.canExecuteDataAlter(entry->alter_version, state_lock))
            {
                int head_alter = alter_sequence.getHeadAlterVersion(state_lock);
                if (head_alter == entry->alter_version)
                    LOG_TRACE(log, "Cannot execute alter data with version {}: metadata not altered", entry->alter_version);
                else
                    LOG_TRACE(log, "Cannot execute alter data with version {} blocked by alter {} ", entry->alter_version, head_alter);
                continue;
            }
        }

        float backoff_exp = std::pow(storage_settings->ha_log_select_interval_multiplier.value, entry->num_tries);
        time_t next_attemp_time = entry->last_attempt_time
            + time_t(std::min(
                storage_settings->ha_max_log_select_interval.value, storage_settings->ha_min_log_select_interval.value * backoff_exp));
        if (now < next_attemp_time)
            continue;

        if (!canExecuteClone(entry))
            continue;

        if ((entry->type == LogEntry::GET_PART || entry->type == LogEntry::CLONE_PART) && entry->num_tries == 0)
            num_avail_fetches -= 1;

        res->setExecuting(entry);
        /// We gave a chance for the entry or its parent, move it to the tail of the queue
        unprocessed_queue.splice(unprocessed_queue.end(), unprocessed_queue, it);
        break;
    }

    return res;
}

void HaMergeTreeQueue::onLogExecutionError(LogEntryPtr & entry, const std::exception_ptr & exception)
{
    std::lock_guard state_lock(state_mutex);
    entry->last_exception = exception;

    if (entry->type == LogEntry::MUTATE_PART)
    {
        Int64 mutation_version = MergeTreePartInfo::fromPartName(entry->new_parts.front(), format_version).getDataVersion();
        if (auto it = mutations_by_version.find(mutation_version); it != mutations_by_version.end())
        {
            MutationStatus * status = it->second;
            if (!status->is_done)
            {
                status->latest_failed_part = entry->source_parts.front();
                status->latest_failed_part_info = MergeTreePartInfo::fromPartName(status->latest_failed_part, format_version);
                status->latest_fail_time = time(nullptr);
                status->latest_fail_reason = getExceptionMessage(exception, false);
            }
        }
    }
}

void HaMergeTreeQueue::removeProcessedEntry(LogEntryPtr & entry)
{
    removeProcessedEntries({entry});
}

void HaMergeTreeQueue::removeProcessedEntries(const LogEntryVec & entries)
{
    if (entries.empty())
        return;

    getLogManager().markLogEntriesExecuted(entries);

    std::lock_guard state_lock(state_mutex);
    for (auto & entry : entries)
    {
        if (0 == unprocessed_by_lsn.erase(entry->lsn))
        {
            LOG_WARNING(log, "{} not found in the queue which might be removed concurrently.", entry->toString());
        }
        else
        {
            unprocessed_counts[entry->type].fetch_sub(1, std::memory_order_relaxed);
            if (entry->type == LogEntry::MERGE_PARTS && entry->source_replica == storage.replica_name)
                unprocessed_merges_of_self.fetch_sub(1, std::memory_order_relaxed);
            if (entry->type == LogEntry::MUTATE_PART && entry->source_replica == storage.replica_name)
                unprocessed_mutations_of_self.fetch_sub(1, std::memory_order_relaxed);
        }
        if (entry->hasMergeMutateFutureParts())
            merge_mutate_future_parts.remove(entry->lsn, entry->new_parts);

        removeCoveredPartsFromMutation(*entry, state_lock);

        if (entry->type == LogEntry::GET_PART || entry->type == LogEntry::REPLACE_PARTITION)
        {
            unprocessed_inserts_by_time.erase(entry);

            if (unprocessed_inserts_by_time.empty())
                min_unprocessed_insert_time.store(std::numeric_limits<time_t>::max(), std::memory_order_relaxed);
            else
                min_unprocessed_insert_time.store((*unprocessed_inserts_by_time.begin())->create_time, std::memory_order_relaxed);

            max_processed_insert_time = std::max<time_t>(entry->create_time, max_processed_insert_time);
        }
    }
}

HaMergeTreeMutationEntryPtr HaMergeTreeQueue::selectAlterMetadataToProcess(const String & last_processed)
{
    std::lock_guard state_lock(state_mutex);

    for (auto & [znode_name, mutation_status] : mutations_for_alter_metadata)
    {
        if (!last_processed.empty() && znode_name <= last_processed)
            continue;
        if (znode_name <= mutation_pointer)
            continue;

        auto & entry = mutation_status.entry;
        if (!alter_sequence.canExecuteMetaAlter(entry->alter_version, state_lock))
        {
            LOG_DEBUG(
                log,
                "Cannot execute {} with version {} because another alter with data mutation must be executed before", entry->getNameForLogs(), entry->alter_version);
            return {};
        }
        else
        {
            return entry;
        }
    }

    return {};
}

void HaMergeTreeQueue::finishMetadataAlter(const MutationEntryPtr & entry)
{
    std::lock_guard state_lock(state_mutex);
    LOG_DEBUG(log, "Finishing " + entry->getNameForLogs());
    alter_sequence.finishMetadataAlter(entry->alter_version, state_lock);
}

HaMergeTreeQueue::OperationsInQueue HaMergeTreeQueue::countMergesAndPartMutations() const
{
    return OperationsInQueue {
        unprocessed_counts[LogEntry::MERGE_PARTS].load(std::memory_order_relaxed),
        unprocessed_merges_of_self.load(std::memory_order_relaxed),
        unprocessed_mutations_of_self.load(std::memory_order_relaxed),
        unprocessed_counts[LogEntry::MUTATE_PART].load(std::memory_order_relaxed)
    };
}

size_t HaMergeTreeQueue::countUnfinishedMutations(bool include_alter_metadata) const
{
    std::lock_guard lock(state_mutex);
    size_t count = 0;
    for (const auto & pair : mutations_by_znode)
    {
        if (!pair.second.is_done)
            ++count;
    }
    if (include_alter_metadata)
    {
        for (const auto & pair : mutations_for_alter_metadata)
        {
            if (!pair.second.is_done)
                ++count;
        }
    }
    return count;
}

size_t HaMergeTreeQueue::countFinishedMutations(bool include_alter_metadata) const
{
    std::lock_guard lock(state_mutex);
    size_t count = 0;
    for (const auto & pair : mutations_by_znode)
    {
        const auto & mutation = pair.second;
        if (!mutation.is_done)
            break;
        ++count;
    }
    if (include_alter_metadata)
    {
        for (const auto & pair : mutations_for_alter_metadata)
        {
            const auto & mutation = pair.second;
            if (!mutation.is_done)
                break;
            ++count;
        }
    }
    return count;
}

HaMergeTreeMutationEntryPtr HaMergeTreeQueue::findDuplicateMutationByQueryId(
    zkutil::ZooKeeperPtr & zookeeper, const MutationEntry & entry, String & max_znode_checked) const
{
    MutationEntryPtr res;
    /// first check loaded entries
    {
        std::lock_guard lock(state_mutex);
        for (auto it = mutations_by_znode.upper_bound(max_znode_checked); it != mutations_by_znode.end(); ++it)
        {
            if (entry.duplicateWith(*it->second.entry))
            {
                return it->second.entry;
            }
        }
        for (auto it = mutations_for_alter_metadata.upper_bound(max_znode_checked); it != mutations_for_alter_metadata.end(); ++it)
        {
            if (entry.duplicateWith(*it->second.entry))
            {
                return it->second.entry;
            }
        }

        if (auto it = mutations_by_znode.rbegin(); it != mutations_by_znode.rend() && max_znode_checked < it->first)
            max_znode_checked = it->first;
        if (auto it = mutations_for_alter_metadata.rbegin(); it != mutations_for_alter_metadata.rend() && max_znode_checked < it->first)
            max_znode_checked = it->first;
    }
    /// then check unloaded entries in zk
    Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/mutations");
    std::sort(entries.begin(), entries.end());
    for (auto it = std::upper_bound(entries.begin(), entries.end(), max_znode_checked); it != entries.end(); ++it)
    {
        String data = zookeeper->get(zookeeper_path + "/mutations/" + *it);
        auto to_check = HaMergeTreeMutationEntry::parse(data, *it);
        if (entry.duplicateWith(to_check))
            return std::make_shared<HaMergeTreeMutationEntry>(std::move(to_check));
        max_znode_checked = *it;
    }

    return {}; /// not found
}

Int64 HaMergeTreeQueue::getCurrentMutationVersionImpl(
    const String & /*partition_id*/, Int64 data_version, std::lock_guard<std::mutex> & /*state_lock*/) const
{
    if (mutations_by_version.empty())
        return 0;
    auto it = mutations_by_version.upper_bound(data_version);
    if (it == mutations_by_version.begin())
        return 0;
    --it;
    return it->first;
}

Int64 HaMergeTreeQueue::getCurrentMutationVersion(const String & partition_id, Int64 data_version) const
{
    std::lock_guard lock(state_mutex);
    return getCurrentMutationVersionImpl(partition_id, data_version, lock);
}

HaMergeTreeMutationEntryPtr HaMergeTreeQueue::getMutationForHangCheck() const
{
    std::lock_guard lock(state_mutex);
    for (const auto & pair : mutations_by_znode)
    {
        if (!pair.second.is_done)
        {
            auto & entry = pair.second.entry;
            if (pair.second.parts_to_do.size() == 0)
                return nullptr; /// no local parts to mutate, can't hang
            if (entry->isAlterMutation() && !alter_sequence.canExecuteDataAlter(entry->alter_version, lock))
                return nullptr; /// haven't started, can't hang
            return entry; /// maybe hang due to conflict parts with leader (mutate log creator)
        }
    }
    return nullptr;
}

HaMergeTreeMutationEntryPtr HaMergeTreeQueue::getExecutablePartsToMutate(const String & mutation_id, Strings & out_parts) const
{
    std::lock_guard lock(state_mutex);
    auto it = mutations_by_znode.find(mutation_id);
    if (it == mutations_by_znode.end())
        return nullptr;
    if (it->second.is_done)
        return nullptr;
    auto & entry = it->second.entry;
    if (entry->isAlterMutation() && !alter_sequence.canExecuteDataAlter(entry->alter_version, lock))
        return nullptr;
    out_parts = it->second.parts_to_do.getParts();
    return entry;
}

Strings HaMergeTreeQueue::getPartsToMutateWithoutLogs(const String & mutation_id) const
{
    Strings parts_to_do;
    HaMergeTreeFutureParts future_parts_copy(storage.format_version);
    {
        std::lock_guard lock(state_mutex);
        auto it = mutations_by_znode.find(mutation_id);
        if (it == mutations_by_znode.end())
            return {};
        if (it->second.is_done || it->second.parts_to_do.size() == 0)
            return {};
        /// copy to avoid holding the lock for too long
        parts_to_do = it->second.parts_to_do.getParts();
        future_parts_copy = merge_mutate_future_parts;
    }

    Strings res;
    for (auto & part : parts_to_do)
    {
        auto info = MergeTreePartInfo::fromPartName(part, storage.format_version);
        if (!future_parts_copy.hasContainingPart(info))
            res.push_back(part);
    }
    return res;
}

MutationCommands HaMergeTreeQueue::getMutationCommands(Int64 mutation_version)
{
    /// if we haven't loaded the mutation entry, try load it first
    if (mutation_version > max_loaded_mutation_version)
    {
        std::lock_guard update_mutations_lock(update_mutations_mutex);
        /// maybe a concurrent task has loaded the entry, double check after acquiring the lock
        if (mutation_version > max_loaded_mutation_version)
        {
            auto zookeeper = storage.getZooKeeper();

            LOG_DEBUG(
                log,
                "The request version {} > max loaded mutation version {}, need to update mutation first",
                mutation_version,
                max_loaded_mutation_version.load(std::memory_order_relaxed));
            updateMutations(zookeeper, update_mutations_lock);
        }
    }

    std::lock_guard lock(state_mutex);
    auto it = mutations_by_version.find(mutation_version);
    if (it == mutations_by_version.end())
    {
        /// mutation was killed
        return MutationCommands{};
    }

    return it->second->entry->commands;
}

bool HaMergeTreeQueue::tryFinalizeMutations(zkutil::ZooKeeperPtr zookeeper)
{
    bool is_log_synced = false;
    HaMergeTreeMutationEntryPtr candidate;
    {
        std::lock_guard lock(state_mutex);
        /// For alter metadata, we must find the first one that:
        for (auto & [znode, mutation] : mutations_for_alter_metadata)
        {
            /// 1) larger than mutation_pointer
            if (mutation.entry->znode_name <= mutation_pointer)
                continue;

            if (mutation.is_done)
                continue;
            /// 2) first elem not done
            candidate = mutation.entry;
            break;
        }

        for (auto & kv : mutations_by_znode)
        {
            MutationStatus & mutation = kv.second;
            if (candidate && mutation.entry->znode_name > candidate->znode_name)
                break;

            if (mutation.is_done)
                continue;

            if (mutation.parts_to_do.size() == 0)
            {
                candidate = mutation.entry;
                is_log_synced = mutation.is_log_synced;
                LOG_DEBUG(log, "Will check if mutation {} is done", candidate->getNameForLogs());
            }
            /// currently mutations are executed one by one.
            /// so if the current mutation is not finished, we don't need to check further mutations
            break;
        }
    }

    if (!candidate)
        return false; /// no unfinished mutation or mutation still has committed part to mutate

    if (candidate->isAlterMetadata())
    {
        /// When failed to execute the operation that set metadata_version on zk
        if (storage.metadata_version < candidate->alter_version)
            return false;
    }
    else
    {
        /// check whether we have future part to mutate

        if (!is_log_synced)
        {
            if (pullLogsToQueue(zookeeper))
            {
                is_log_synced = true;
                std::lock_guard lock(state_mutex);
                if (auto it = mutations_by_version.find(candidate->block_number); it != mutations_by_version.end())
                    it->second->is_log_synced = true;
            }
        }

        if (!is_log_synced)
        {
            LOG_DEBUG(log, "Can't finalize mutation " + candidate->getNameForLogs()
                          + " because logs are not up-to-date, may have future part to mutate");
            return false;
        }

        std::lock_guard lock(state_mutex);
        auto it = mutations_by_version.find(candidate->block_number);
        if (it == mutations_by_version.end())
        {
            LOG_DEBUG(log, "Can't finalize mutation " + candidate->getNameForLogs()
                           + " because mutation is killed and removed from memory");
            return false;
        }

        MutationStatus * status = it->second;
        if (status->parts_to_do.size() > 0)
        {
            LOG_DEBUG(log, "Can't finalize mutation " + candidate->getNameForLogs()
                           + " because " + toString(status->parts_to_do.size()) + " parts to mutate suddenly appeared.");
            return false;
        }

        for (auto & entry : unprocessed_queue)
        {
            if (entry->type == LogEntry::GET_PART || entry->type == LogEntry::CLONE_PART)
            {
                MergeTreePartInfo part_info = MergeTreePartInfo::fromPartName(entry->new_parts.front(), storage.format_version);
                if (part_info.getDataVersion() < candidate->block_number && candidate->coverPartitionId(part_info.partition_id))
                {
                    LOG_DEBUG(log, "Can't finalize mutation " + candidate->getNameForLogs()
                                   + " because found future part " + entry->new_parts.front() + " to mutate");
                    return false;
                }
            }
        }
    }

    zookeeper->set(replica_path + "/mutation_pointer", candidate->znode_name);
    LOG_INFO(log, "Updated mutation pointer from " + mutation_pointer + " to " + candidate->znode_name);

    bool may_execute_alter_metadata = false;

    {
        std::lock_guard lock(state_mutex);
        mutation_pointer = candidate->znode_name;
        if (auto it = mutations_by_znode.find(candidate->znode_name); it != mutations_by_znode.end())
        {
            if (candidate->isAlterMutation())
            {
                LOG_DEBUG(log, "Finishing " + candidate->getNameForLogs());
                alter_sequence.finishDataAlter(candidate->alter_version, lock);
                may_execute_alter_metadata = alter_sequence.hasUnfinishedMetadataAlter(lock);
            }
            LOG_INFO(log, "Mutation " + candidate->getNameForLogs() + " is done on this replica");
            it->second.is_done = true;
            it->second.finish_time = time(nullptr);
        }

        if (auto it = mutations_for_alter_metadata.find(candidate->znode_name); it != mutations_for_alter_metadata.end())
        {
            LOG_INFO(log, "Mutation " + candidate->getNameForLogs() + " is done on this replica");
            it->second.is_done = true;
            it->second.finish_time = time(nullptr);

            may_execute_alter_metadata = alter_sequence.hasUnfinishedMetadataAlter(lock);
        }
    }

    if (may_execute_alter_metadata)
        storage.alter_thread.start();

    storage.writeMutationLog(MutationLogElement::MUTATION_FINISH, *candidate);
    return true;
}

HaMergeTreeQueue::Status HaMergeTreeQueue::getStatus() const
{
    /*
    Status status;
    for (size_t i = 0; i < unprocessed_counts.size(); ++i)
        status.unprocessed_counts[i] = unprocessed_counts[i];
    status.min_unprocessed_insert_time = min_unprocessed_insert_time;
    // status.max_processed_insert_time = max_processed_insert_time;
    status.last_queue_update_time = last_queue_update_time;
    */

    std::lock_guard state_lock(state_mutex);

    Status status;

    status.queue_size = unprocessed_queue.size();
    status.clones_in_queue = unprocessed_counts[LogEntry::CLONE_PART].load(std::memory_order_relaxed);
    status.inserts_in_queue = unprocessed_counts[LogEntry::GET_PART].load(std::memory_order_relaxed);
    status.merges_in_queue = unprocessed_counts[LogEntry::MERGE_PARTS].load(std::memory_order_relaxed);
    status.merges_of_self = unprocessed_merges_of_self.load(std::memory_order_relaxed);
    status.part_mutations_in_queue = unprocessed_counts[LogEntry::MUTATE_PART].load(std::memory_order_relaxed);
    status.part_mutations_of_self = unprocessed_mutations_of_self.load(std::memory_order_relaxed);

    status.last_queue_update = last_queue_update_time.load(std::memory_order_relaxed);

    std::array<time_t, LogEntry::TypesCount> oldest_time {0};
    std::array<String, LogEntry::TypesCount> oldest_part;

    for (auto & entry : unprocessed_queue)
    {
        if (status.queue_oldest_time == 0 || entry->create_time < status.queue_oldest_time)
            status.queue_oldest_time = entry->create_time;

        if (oldest_time[entry->type] == 0 || entry->create_time < oldest_time[entry->type])
        {
            oldest_time[entry->type] = entry->create_time;
            oldest_part[entry->type] = entry->formatNewParts();
        }
    }

    status.clones_oldest_time = oldest_time[LogEntry::CLONE_PART];
    status.inserts_oldest_time = oldest_time[LogEntry::GET_PART];
    status.merges_oldest_time = oldest_time[LogEntry::MERGE_PARTS];
    status.part_mutations_oldest_time = oldest_time[LogEntry::MUTATE_PART];

    status.oldest_part_to_clone = oldest_part[LogEntry::CLONE_PART];
    status.oldest_part_to_get = oldest_part[LogEntry::GET_PART];
    status.oldest_part_to_merge_to = oldest_part[LogEntry::MERGE_PARTS];
    status.oldest_part_to_mutate_to = oldest_part[LogEntry::MUTATE_PART];

    return status;
}

std::pair<time_t, time_t> HaMergeTreeQueue::getAbsoluteDelay() const
{
    time_t current_time = time(nullptr);
    time_t curr_min_unprocessed_insert_time = this->min_unprocessed_insert_time.load(std::memory_order_relaxed);
    // this variable is to count latest lsn being processed in this replica, and designed to help log lag case.
    time_t curr_max_processed_insert_time = this->max_processed_insert_time;
    curr_max_processed_insert_time = current_time > curr_max_processed_insert_time ? current_time - curr_max_processed_insert_time : 0;
    time_t curr_last_queue_update_time = this->last_queue_update_time.load(std::memory_order_relaxed);
    size_t num_unprocessed_clone = unprocessed_counts[LogEntry::CLONE_PART].load(std::memory_order_relaxed);

    if (!curr_last_queue_update_time || num_unprocessed_clone > 0)
    {
        return {VERY_LARGE_DELAY, curr_max_processed_insert_time};
    }
    else if (curr_min_unprocessed_insert_time != std::numeric_limits<time_t>::max())
    {
        return {current_time > curr_min_unprocessed_insert_time ? current_time - curr_min_unprocessed_insert_time : 0,
                curr_max_processed_insert_time};
    }
    /*
    else if (current_time - curr_last_queue_update_time > time_t(storage.settings.ha_queue_update_sleep_ms * 2))
    {
        return {current_time - curr_last_queue_update_time, curr_max_processed_insert_time};
    }
    */
    else
    {
        return {0, curr_max_processed_insert_time};
    }
}

std::vector<MergeTreeMutationStatus> HaMergeTreeQueue::getMutationsStatus() const
{
    std::lock_guard lock(state_mutex);

    std::vector<MergeTreeMutationStatus> result;
    for (const auto & pair : mutations_by_znode)
    {
        const MutationStatus & status = pair.second;
        const HaMergeTreeMutationEntry & entry = *status.entry;
        Names parts_to_mutate = status.parts_to_do.getParts();
        std::map<String, Int64> block_numbers_map({{"", entry.block_number}});

        for (const MutationCommand & command : entry.commands)
        {
            WriteBufferFromOwnString buf;
            formatAST(*command.ast, buf, false, true);
            result.push_back(MergeTreeMutationStatus
            {
                entry.znode_name,
                entry.query_id,
                buf.str(),
                entry.create_time,
                block_numbers_map,
                parts_to_mutate,
                status.is_done,
                status.finish_time,
                status.latest_failed_part,
                status.latest_fail_time,
                status.latest_fail_reason,
            });
        }
    }

    return result;
}

std::optional<MergeTreeMutationStatus> HaMergeTreeQueue::getPartialMutationsStatus(
    const String & znode_name, bool is_alter_metadata, String * out_mutation_pointer) const
{
    const MutationStatus * status = nullptr;
    std::lock_guard lock(state_mutex);

    if (out_mutation_pointer)
        *out_mutation_pointer = mutation_pointer;

    if (is_alter_metadata)
    {
        auto it = mutations_for_alter_metadata.find(znode_name);
        if (it != mutations_for_alter_metadata.end())
            status = &(it->second);
    }
    else
    {
        auto it = mutations_by_znode.find(znode_name);
        if (it != mutations_by_znode.end())
            status = &(it->second);
    }

    if (!status)
        return {}; /// killed
    return MergeTreeMutationStatus
        {
            .is_done = status->is_done,
            .finish_time = status->finish_time,
            .latest_failed_part = status->latest_failed_part,
            .latest_fail_time = status->latest_fail_time,
            .latest_fail_reason = status->latest_fail_reason,
        };
}

HaMergeTreeQueue::LogEntryVec HaMergeTreeQueue::lookupEntriesByLSN(const LSNList & lsns)
{
    std::lock_guard state_lock(state_mutex);

    LogEntryVec entries;
    entries.reserve(lsns.size());
    for (auto lsn : lsns)
    {
        if (auto iter = unprocessed_by_lsn.find(lsn); iter != unprocessed_by_lsn.end())
            entries.push_back(*iter);
    }
    return entries;
}

void HaMergeTreeQueue::getUnprocessedEntries(std::vector<HaMergeTreeLogEntryData> & entries) const
{
    std::lock_guard state_lock(state_mutex);

    entries.reserve(unprocessed_queue.size());
    for (auto & entry : unprocessed_queue)
        entries.emplace_back(*entry);
}

size_t HaMergeTreeQueue::getQueueSize() const
{
    std::lock_guard state_lock(state_mutex);
    return unprocessed_queue.size();
}

[[maybe_unused]]static std::vector<std::pair<UInt64, UInt64>> getTimeInterval(const String & times)
{
    String time_interval_str = times;
    boost::trim_if(time_interval_str, boost::is_any_of(" "));
    std::vector<String> time_intervals_vec;
    if (!time_interval_str.empty())
        boost::split(time_intervals_vec, time_interval_str, boost::is_any_of(","), boost::token_compress_on);
    std::vector<std::pair<UInt64, UInt64>> time_intervals;

    if (time_intervals_vec.size() % 2 == 0)
    {
        for (size_t i = 0; i < time_intervals_vec.size(); i += 2)
        {
            UInt64 start = std::stoi(time_intervals_vec[i]);
            UInt64 end = std::stoi(time_intervals_vec[i+1]);

            time_intervals.emplace_back(start, end);
        }
    }
    else
    {
        LOG_DEBUG(&Poco::Logger::get("getTimeInterval"), "Error config for clone strategy, the size of time interval should be multiple of 2");
    }

    return time_intervals;
}

void HaMergeTreeQueue::checkAddMetadataAlter(const MutationCommands & commands) const
{
    std::lock_guard state_lock(state_mutex);
    if (auto column = alter_sequence.canAddMetadataAlter(commands, state_lock))
        throw Exception(
            "There are unfinished async ALTERs which might confict on column `" + *column + "`, please wait...",
            ErrorCodes::CANNOT_ASSIGN_ALTER);

    if (commands.willMutateData() && alter_sequence.hasUnfinishedDataAlter(state_lock))
        throw Exception("There are unfinished async ALTERs with data mutation, please wait...", ErrorCodes::CANNOT_ASSIGN_ALTER);
}

HaMergeTreeMergePredicate HaMergeTreeQueue::getMergePredicate(zkutil::ZooKeeperPtr & zookeeper)
{
    return HaMergeTreeMergePredicate(*this, zookeeper);
}

static bool hasNewParts(const HaMergeTreeLogEntry & entry)
{
    return entry.type == HaMergeTreeLogEntry::MERGE_PARTS || entry.type == HaMergeTreeLogEntry::MUTATE_PART
        || entry.type == HaMergeTreeLogEntry::GET_PART || entry.type == HaMergeTreeLogEntry::CLONE_PART
        || entry.type == HaMergeTreeLogEntry::REPLACE_PARTITION;
}

HaMergeTreeMergePredicate::HaMergeTreeMergePredicate(HaMergeTreeQueue & queue_, zkutil::ZooKeeperPtr & zookeeper)
    : queue(queue_)
      , future_merge_parts(queue.storage.format_version)
      , future_parts(queue.storage.format_version)
      , snapshot_time(time(nullptr))
      , log(&Poco::Logger::get(queue.storage.getStorageID().getNameForLogs() + " (MergePredicate)"))
{
    Stopwatch watch;
    // actively sync up-to-date logs
    queue.pullLogsToQueue(zookeeper);

    auto pull_logs_millis = watch.elapsedMilliseconds();


    /// The main idea is that take a snapshot of log entries and calc the `interval`
    /// for committed parts according the log snapshot. And two parts can merge only
    /// if they belongs to same `interval`.
    ///
    /// Because every part is associated with a log: the log not executed marks some
    /// future parts; a unknown log indicates some unknown parts; and an executed log
    /// marks some committed parts. So the lsn gap between executed logs could divide
    /// committed parts into `intervals`, and the future/unknown parts would be marked
    /// as `INTERVAL_GAP`.
    ///
    /// But there are still some parts of which logs may be too old to cleared or not
    /// sync-ed in time. To distinguish them, we compare snapshot time (ST) of merge predicate
    /// and modification time (MT) of part.
    ///
    /// For a part not belongs to any interval:
    /// 1. If MT < ST, the log associated with this part is written before the snapshot,
    ///    so must be already cleared. Mark it as `interval-0` for convenience.
    /// 2. If MT >= ST, the log is written after snapshot, i.e. the part is unknown for
    ///    this snapshot and mark it as `INTERVAL_GAP`.

    auto & log_manager = queue.getLogManager();
    auto log_lock = log_manager.lockMe();

    auto last_lsn = log_manager.getLSNStatusUnsafe(true).updated_lsn;
    UInt32 interval = 0;

    log_manager.applyToEntriesFromUpdatedLSNUnsafe([&](const HaMergeTreeLogEntryPtr & entry) {
        if (!entry->is_executed)
        {
            if (hasNewParts(*entry))
            {
                for (auto & part : entry->new_parts)
                    part_to_interval.try_emplace(part, INTERVAL_GAP);
                for (auto & part : entry->new_parts)
                    future_parts.add(part);
            }

            if (entry->type == HaMergeTreeLogEntry::MERGE_PARTS)
            {
                for (auto & part : entry->new_parts)
                    future_merge_parts.add(part);
            }
        }
        else
        {
            /// Update interval index first if here is a gap
            if (entry->lsn - last_lsn > 1)
                interval += 1;

            /// Put new_parts into current interval
            /// Do not care parts in `interval-0`
            if (interval > 0 && hasNewParts(*entry))
            {
                for (auto & part : entry->new_parts)
                    part_to_interval.try_emplace(part, interval);
            }
        }

        /// Move ahead
        last_lsn = entry->lsn;
    });

    if (auto elapsed = watch.elapsedMilliseconds(); elapsed > 1000)
    {
        LOG_DEBUG(queue.log, "Create MergePredicate took {} ms ({} ms in pull logs to queue)", elapsed, pull_logs_millis);
    }
}

Int32 HaMergeTreeMergePredicate::findMergeInterval(const MergeTreeData::DataPartPtr & part) const
{
    if (auto iter = part_to_interval.find(part->name); iter == part_to_interval.end())
    {
        if (part->modification_time >= snapshot_time)
            return INTERVAL_GAP;
        else
            return 0;
    }
    else
    {
        return iter->second;
    }
}

bool HaMergeTreeMergePredicate::operator()(
    const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right, String *) const
{
    if (!left)
        return true; /// TODO:

    bool enable_logging = true;

    if (left->info.partition_id != right->info.partition_id)
    {
        if (enable_logging)
            LOG_TRACE(log, "{} & {}: Parts belong to different partitions", left->name, right->name);
        return false;
    }

    Int64 left_max_block = left->info.max_block;
    Int64 right_min_block = right->info.min_block;

    if (left_max_block > right_min_block)
    {
        if (enable_logging)
            LOG_TRACE(log, "{} & {}: Left part's block id is greater than right part's block id", left->name, right->name);
        return false;
    }

    {
        std::lock_guard lock(queue.state_mutex);
        /// check whether we have created any log to merge/mutate/delete the part
        for (auto & part : {left, right})
        {
            String containing_part;
            if (queue.merge_mutate_future_parts.hasContainingPart(part->info, &containing_part))
            {
                if (enable_logging)
                    LOG_TRACE(log, "{} is covered by future parts ", part->name, containing_part);
                return false;
            }
        }

        /// we can't merge parts of different mutation version, otherwise the merged part will lost some mutation forever
        auto lhs_mutation_ver = queue.getCurrentMutationVersionImpl(left->info.partition_id, left->info.getDataVersion(), lock);
        auto rhs_mutation_ver = queue.getCurrentMutationVersionImpl(right->info.partition_id, right->info.getDataVersion(), lock);
        if (lhs_mutation_ver != rhs_mutation_ver)
        {
            if (enable_logging)
                LOG_TRACE(
                    log,
                    "Current mutation version of parts {} and {} differs: {} and {} respectively",
                    left->name,
                    right->name,
                    toString(lhs_mutation_ver),
                    toString(rhs_mutation_ver));
            return false;
        }
    }

    // NOTE: Assume input left, right are closest parts in this partition, and this function
    // doesn't need to iterator local parts to bypass overlap one.
    if (left_max_block + 1 < right_min_block)
    {
        /// XXX: Is it necessary to check future_parts ? Could these cases be covered by
        /// `zero level & non-zero interval` checking ?
        MergeTreePartInfo gap_part_info(
            left->info.partition_id,
            left_max_block + 1,
            right_min_block - 1,
            MergeTreePartInfo::MAX_LEVEL,
            MergeTreePartInfo::MAX_BLOCK_NUMBER);
        auto covered = future_parts.getPartsCoveredBy(gap_part_info);
        if (!covered.empty())
        {
            if (enable_logging)
                LOG_DEBUG(log, "{} & {} : There are gap parts not fetched", left->name, right->name);
            return false;
        }

        auto left_interval = findMergeInterval(left);
        auto right_interval = findMergeInterval(right);

        if (left_interval == INTERVAL_GAP)
        {
            if (enable_logging)
                LOG_DEBUG(log, "left part {} interval can not be determined", left->name);
            return false;
        }
        if (right_interval == INTERVAL_GAP)
        {
            if (enable_logging)
                LOG_DEBUG(log, "right part {} interval can not be determined", right->name);
            return false;
        }

        if (left_interval != right_interval)
        {
            if (enable_logging)
                LOG_DEBUG(
                    log,
                    "{} & {}: Left part and right part are not belong to same merge interval: {}",
                    left->name,
                    right->name,
                    right_interval);
            return false;
        }

        /// TODO:
        /// if (queue.storage.global_context.getSettingsRef().conservative_merge_predicate)
        /// {
        ///     /// `left_interval == right_interval` now
        ///     /// XXX: This may block high level merge for a long time until the LSN gap is filled.
        ///     if (left_interval > 0 && (left->info.level > 0 || right->info.level > 0))
        ///     {
        ///         if (enable_logging)
        ///             LOG_WARNING(
        ///                 log, "{} & {}: only level-0 parts in non-zero interval can merge; Please check log hole.", left->name, right->name);
        ///         return false;
        ///     }
        /// }
    }

    return true;
}

std::optional<std::pair<Int64, int>> HaMergeTreeMergePredicate::getDesiredMutationVersion(const MergeTreeData::DataPartPtr & part) const
{
    std::lock_guard lock(queue.state_mutex);

    if (queue.mutations_by_version.empty())
        return {};

    if (queue.merge_mutate_future_parts.hasContainingPart(part->info))
        return {};

    Int64 current_version = queue.getCurrentMutationVersionImpl(part->info.partition_id, part->info.getDataVersion(), lock);
    Int64 max_version = current_version;

    int alter_version = -1;
    for (auto [mutation_version, mutation_status] : queue.mutations_by_version)
    {
        /// Skip unnecessary mutation entries according filtering partition ids.
        if (mutation_status->is_done || !mutation_status->entry->coverPartitionId(part->info.partition_id))
            continue;

        /// Mutate part to the first unfinished mutation whose version > part's current version.
        /// In addition, it doesn't make sense to assign more fresh alter mutation if previous alter
        /// is still running because alters execute one by one in strict order.
        if (mutation_version > current_version || mutation_status->entry->isAlterMutation())
        {
            max_version = mutation_version;
            alter_version = mutation_status->entry->alter_version;
            break;
        }
    }

    if (current_version >= max_version)
        return {};

    return std::make_pair(max_version, alter_version);
}
}
