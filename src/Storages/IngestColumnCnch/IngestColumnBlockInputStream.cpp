#include <Storages/IngestColumnCnch/IngestColumnBlockInputStream.h>
#include <Storages/IngestColumnCnch/IngestColumnHelper.h>
#include <CloudServices/CnchWorkerResource.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/IStorage.h>
#include <Catalog/Catalog.h>
#include <Storages/IngestColumnCnch/IngestColumnHelper.h>
#include <Storages/IngestColumnCnch/memoryEfficientIngestColumn.h>
#include <CloudServices/CnchPartsHelper.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
}

namespace
{

MergeTreeDataPartsVector getAndLoadPartsInWorker(
    Catalog::Catalog & catalog,
    StoragePtr storage,
    StorageCloudMergeTree & cloud_merge_tree,
    const String & partition_id,
    TxnTimestamp ts,
    ContextPtr local_context)
{
    ServerDataPartsVector server_parts = catalog.getServerDataPartsInPartitions(storage, {partition_id}, ts, local_context.get());

    pb::RepeatedPtrField<Protos::DataModelPart> parts_model;
    StoragePtr storage_for_cnch_merge_tree = catalog.tryGetTable(
        *local_context,
        cloud_merge_tree.getCnchDatabase(),
        cloud_merge_tree.getCnchTable(),
        local_context->getCurrentCnchStartTime());

    if (!storage_for_cnch_merge_tree)
        throw Exception("Fail to get storage from catalog", ErrorCodes::SYSTEM_ERROR);

    fillPartsModelForSend(*storage_for_cnch_merge_tree, server_parts, parts_model);

    MergeTreeMutableDataPartsVector unloaded_parts =
        createPartVectorFromModelsForSend<MergeTreeMutableDataPartPtr>(cloud_merge_tree, parts_model);

    cloud_merge_tree.loadDataParts(unloaded_parts, false);

    MergeTreeDataPartsVector res;
    std::move(unloaded_parts.begin(), unloaded_parts.end(),
        std::back_inserter(res));
    return res;
}

}

void IngestColumnBlockInputStream::logIngestWithBucketStatus()
{
    LOG_TRACE(
        log,
        fmt::format(
            "Ingest Column with bucket: {}, cur_bucket_index {} with source parts: {}/{}  target parts: {}/{}",
            buckets_for_ingest[cur_bucket_index],
            cur_bucket_index,
            getCurrentVisibleSourceParts().size(),
            visible_source_parts.size(),
            getCurrentVisibleTargetParts().size(),
            visible_target_parts.size()));
}

IngestColumnBlockInputStream::IngestColumnBlockInputStream(
    StoragePtr target_storage_,
    const PartitionCommand & command,
    ContextPtr local_context
)
    : target_storage{std::move(target_storage_)},
      context(std::move(local_context)),
      log(getLogger(target_storage->getStorageID().getNameForLogs() + " (IngestColumn)"))
{
    source_storage = context->tryGetCnchWorkerResource()->getTable(StorageID{command.from_database, command.from_table});

    ingest_column_names = command.column_names;
    target_cloud_merge_tree = dynamic_cast<StorageCloudMergeTree *>(target_storage.get());
    if (!target_cloud_merge_tree)
        throw Exception("target table in worker is not CloudMeregTree", ErrorCodes::LOGICAL_ERROR);
    source_cloud_merge_tree = dynamic_cast<StorageCloudMergeTree *>(source_storage.get());

    if (!source_cloud_merge_tree)
        throw Exception("source table in worker is not CloudMeregTree", ErrorCodes::LOGICAL_ERROR);

    auto target_meta_data_ptr = target_cloud_merge_tree->getInMemoryMetadataPtr();
    auto source_meta_data_ptr = source_cloud_merge_tree->getInMemoryMetadataPtr();
    target_storage_snapshot = target_cloud_merge_tree->getStorageSnapshot(target_meta_data_ptr, local_context);
    source_storage_snapshot = source_cloud_merge_tree->getStorageSnapshot(source_meta_data_ptr, local_context);

    ordered_key_names = getOrderedKeys(command.key_names, *target_meta_data_ptr);
    partition_id = target_cloud_merge_tree->getPartitionIDFromQuery(command.partition, context);

    std::shared_ptr<Catalog::Catalog> catalog = context->getCnchCatalog();
    source_parts = getAndLoadPartsInWorker(
        *catalog, source_storage, *source_cloud_merge_tree, partition_id, context->getCurrentCnchStartTime(), context);
    LOG_DEBUG(log, "number of source parts: {}", source_parts.size());
    /// ingest partition is a read-write txn, and the default RC isolation level will lead to duplicated keys
    /// (e.g., running two ingest txns which add the same new key concurrently)
    /// therefore should use new ts when fetching target table's parts
    target_parts = getAndLoadPartsInWorker(
        *catalog, target_storage, *target_cloud_merge_tree, partition_id, context->getTimestamp(), context);
    LOG_DEBUG(log, "number of target parts: {}", target_parts.size());

    visible_source_parts = CnchPartsHelper::calcVisibleParts(source_parts, false, CnchPartsHelper::EnableLogging);
    LOG_DEBUG(log, "number of visible source parts: {}", visible_source_parts.size());
    visible_target_parts = CnchPartsHelper::calcVisibleParts(target_parts, false, CnchPartsHelper::EnableLogging);
    LOG_DEBUG(log, "number of visible target parts: {}", visible_target_parts.size());

    if (context->getSettingsRef().optimize_ingest_with_bucket
        && checkIngestWithBucketTable(*source_cloud_merge_tree, *target_cloud_merge_tree, ordered_key_names, ingest_column_names))
    {
        if (command.bucket_nums.empty())
            throw Exception("Receive empty bucket for ingest on worker", ErrorCodes::LOGICAL_ERROR);

        LOG_TRACE(log, "try ingest with bucket table");
        cur_bucket_index = 0;
        buckets_for_ingest = command.bucket_nums;
        visible_source_parts_with_bucket = clusterDataPartWithBucketTable(*source_cloud_merge_tree, visible_source_parts);
        visible_target_parts_with_bucket = clusterDataPartWithBucketTable(*target_cloud_merge_tree, visible_target_parts);

        if (visible_source_parts_with_bucket.empty() || visible_target_parts_with_bucket.empty())
        {
            cur_bucket_index = -1;
            LOG_DEBUG(log, "try ingest with bucket table Failed, use ordinary ingest.");
        }
    }
}


IMergeTreeDataPartsVector & IngestColumnBlockInputStream::getCurrentVisibleSourceParts()
{
    if (cur_bucket_index == -1)
    {
        return visible_source_parts;
    }
    return visible_source_parts_with_bucket[buckets_for_ingest[cur_bucket_index]];
}

IMergeTreeDataPartsVector & IngestColumnBlockInputStream::getCurrentVisibleTargetParts()
{
    if (cur_bucket_index == -1)
    {
        return visible_target_parts;
    }
    return visible_target_parts_with_bucket[buckets_for_ingest[cur_bucket_index]];
}


Block IngestColumnBlockInputStream::readImpl()
{
    if (cur_bucket_index == -1)
    {
        MemoryEfficientIngestColumn executor{*this};
        executor.execute();
    }
    else
    {
        for (; static_cast<size_t>(cur_bucket_index) < buckets_for_ingest.size(); cur_bucket_index++)
        {
            logIngestWithBucketStatus();

            if (getCurrentVisibleSourceParts().empty())
                continue;

            MemoryEfficientIngestColumn executor{*this};
            executor.execute();
        }
    }

    return Block{};
}
}
