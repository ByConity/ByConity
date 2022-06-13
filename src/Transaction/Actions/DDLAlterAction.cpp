#include <Transaction/Actions/DDLAlterAction.h>

#include <Catalog/Catalog.h>
// #include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
// #include <Storages/StorageCnchMergeTree.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void DDLAlterAction::setNewSchema(String schema_)
{
    new_schema = schema_;
}

void DDLAlterAction::setMutationCommmands(MutationCommands commands)
{
    /// Sanity check. Avoid mixing other commands with recluster command.
    // if (commands.size() > 1)
    // {
    //     for (auto & cmd : commands)
    //     {
    //         if (cmd.type == MutationCommand::Type::RECLUSTER)
    //             throw Exception("Cannot modify cluster by definition and other table schema together.", ErrorCodes::LOGICAL_ERROR);
    //     }
    // }
    mutation_commands = std::move(commands);
}

void DDLAlterAction::executeV1(TxnTimestamp commit_time)
{
    /// In DDLAlter, we only update schema.
    LOG_DEBUG(log, "Wait for change schema in Catalog.");
    auto catalog = context.getCnchCatalog();
    try
    {
        // if (!mutation_commands.empty())
        // {
        //     /// TODO: write undo buffer before rename
        //     // updatePartData(part, commit_time);
        //     // part->renameTo(part->info.getPartName(true));

        //     CnchMergeTreeMutationEntry mutation_entry;
        //     mutation_entry.txn_id = txn_id;
        //     mutation_entry.commit_time = commit_time;
        //     mutation_entry.commands = mutation_commands;
        //     mutation_entry.columns_commit_time = mutation_commands.changeSchema() ? commit_time : table->commit_time;
        //     catalog->createMutation(table->getStorageID(), mutation_entry.txn_id.toString(), mutation_entry.toString());
        //     if (table->isBucketTable() && mutation_entry.isReclusteringMutation())
        //         catalog->setTableClusterStatus(table->getStorageUUID(), false);
        //     LOG_DEBUG(log, "Successfully create mutation for alter query.");
        // }

        // auto cache = context.getMaskingPolicyCache();
        // table->checkMaskingPolicy(*cache);

        updateTsCache(table->getStorageUUID(), commit_time);
        // catalog->alterTable(table, new_schema, static_cast<StorageCnchMergeTree &>(*table).commit_time, txn_id, commit_time);
        LOG_DEBUG(log, "Successfully change schema in Catalog.");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        catalog->removeMutation(table->getStorageID(), txn_id.toString());
        throw;
    }
}

void DDLAlterAction::updatePartData(MutableMergeTreeDataPartCNCHPtr part, TxnTimestamp commit_time)
{
    part->commit_time = commit_time;
}

void DDLAlterAction::updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time)
{
    auto & ts_cache_manager = context.getCnchTransactionCoordinator().getTsCacheManager();
    auto table_guard = ts_cache_manager.getTimestampCacheTableGuard(uuid);
    auto & ts_cache = ts_cache_manager.getTimestampCacheUnlocked(uuid);
    ts_cache->insertOrAssign(UUIDHelpers::UUIDToString(uuid), commit_time);
}

}
