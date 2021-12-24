#include <Databases/IDatabase.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/HaUniqueMergeTreeAlterThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/StorageHaUniqueMergeTree.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/setThreadName.h>

#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
    extern const int CANNOT_ASSIGN_ALTER;
}

static const auto ALTER_ERROR_SLEEP_MS = 10 * 1000;


HaUniqueMergeTreeAlterThread::HaUniqueMergeTreeAlterThread(StorageHaUniqueMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getNameForLogs() + " (HaUniqueMergeTreeAlterThread)")
    , log(&Poco::Logger::get(log_name))
{
    task = storage_.getContext()->getSchedulePool().createTask(log_name, [this] { run(); });
}

using TableMetadata = ReplicatedMergeTreeTableMetadata;

void HaUniqueMergeTreeAlterThread::run()
{
    try
    {
        LOG_DEBUG(log, "AlterThread running");

        /// NOTE: we compare the part's schema with the latest manifest schema.
        String old_columns_str = part_columns;
        String old_metadata_str = part_metadata;

        if (old_columns_str.empty() || old_metadata_str.empty())
        {
            throw Exception("AlterThread should have the part schema", ErrorCodes::CANNOT_ASSIGN_ALTER);
        }

        String new_columns_str = storage.manifest_store->getLatestTableColumns();
        String new_metadata_str = storage.manifest_store->getLatestTableMetadata();

        bool changed_columns = (old_columns_str != new_columns_str);
        bool changed_metadata = (old_metadata_str != new_metadata_str);

        if (!changed_columns && !changed_metadata && !force_recheck_parts)
        {
            LOG_INFO(log, "Nothing needs to be altered, return");
            return;
        }

        LOG_DEBUG(log, "AlterThread part schema: {}\n{}", old_metadata_str, old_columns_str);
        LOG_DEBUG(log, "AlterThread manifest schema: {}\n{}", new_metadata_str, new_columns_str);

        // auto new_columns = ColumnsDescription::parse(new_columns_str);
        auto new_metadata = TableMetadata::parse(new_metadata_str);
        auto metadata_snapshot = storage.getInMemoryMetadataPtr();
        auto metadata_diff = TableMetadata(storage, metadata_snapshot).checkAndFindDiff(new_metadata);

        if (metadata_diff.ttl_table_changed)
        {
            // NOTE: Ignore change part level ttl
            return;
        }

        if (metadata_diff.sorting_key_changed || metadata_diff.skip_indices_changed || metadata_diff.constraints_changed)
        {
            throw Exception("Only support modify ttl for unique engine", ErrorCodes::CANNOT_ASSIGN_ALTER);
        }

        /// Because we need to select some parts to alter, thus do not want new parts to keep adding.
        /// so here we suspend part merge process first.
        ActionLock merge_blocker = storage.merger_mutator.merges_blocker.cancel();
        ActionLock moves_blocker = storage.parts_mover.moves_blocker.cancel();

        {
            /// Also stop the current running merges.
            auto write_lock = storage.uniqueWriteLock();
            for (const auto & it : storage.running_merge_states)
            {
                it.second->cancel();
            }
        }

        MergeTreeData::DataParts parts;

        /// If metadata nodes have changed, we will update table structure locally.
        if (changed_columns || changed_metadata)
        {
            /// Temporarily cancel parts sending
            ActionLock data_parts_exchange_blocker;
            if (storage.data_parts_exchange_endpoint_holder)
                data_parts_exchange_blocker = storage.data_parts_exchange_endpoint_holder->getBlocker().cancel();

            /// Temporarily cancel part fetches
            auto fetches_blocker = storage.fetcher.blocker.cancel();

            LOG_INFO(log, "Table schema changed. Waiting for table alter locks");
            auto alter_lock
                = storage.lockForAlter(RWLockImpl::NO_QUERY, storage.getSettings()->lock_acquire_timeout_for_background_operations);

            // This is a lock for altering compression column if it is being recoded
            // MergeTreeData::RecodeDataPartLock lock_for_recode = storage.lockDataForRecode();

            /// You need to get a list of parts under table lock to avoid race condition with merge.
            parts = storage.getDataParts();

            // storage.addRecodeAndBitMapTaskIfNeeded(false);
        }

        /// Update parts.
        if (changed_columns || force_recheck_parts)
        {
            if (changed_columns)
                LOG_INFO(log, "ALTER-ing parts");

            int changed_parts = 0;
            if (!changed_columns)
                parts = storage.getDataParts();

            for (const MergeTreeData::DataPartPtr & part : parts)
            {
                LOG_DEBUG(log, "alterImpl altering part: {}", part->name);

                const auto new_part_columns = metadata_snapshot->columns.getAllPhysical();
                auto transaction = storage.alterDataPartForUniqueTable(part, new_part_columns);

                if (!transaction)
                    continue;

                ++changed_parts;
                // Apply file change
                transaction->commit();
            }

            /// Columns sizes could be quietly changed in case of MODIFY/ADD COLUMN
            storage.calculateColumnSizesImpl();

            if (changed_columns)
            {
                if (changed_parts != 0)
                    LOG_INFO(log, "ALTER-ed {} parts", changed_parts);
                else
                    LOG_INFO(log, "No parts ALTER-ed");
            }

            force_recheck_parts = false;

            /// Update part schema
            part_metadata = new_metadata_str;
            part_columns = new_columns_str;
        }
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;

        force_recheck_parts = true;
        task->scheduleAfter(ALTER_ERROR_SLEEP_MS);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        force_recheck_parts = true;
        task->scheduleAfter(ALTER_ERROR_SLEEP_MS);
    }
}
}
