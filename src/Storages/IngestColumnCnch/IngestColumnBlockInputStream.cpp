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
    ContextPtr local_context)
{
    ServerDataPartsVector server_parts = catalog.getServerDataPartsInPartitions(storage, {partition_id}, local_context->getCurrentCnchStartTime(), local_context.get());

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

IngestColumnBlockInputStream::IngestColumnBlockInputStream(
    StoragePtr target_storage_,
    const PartitionCommand & command,
    ContextPtr local_context
)
    : target_storage{std::move(target_storage_)},
      context(std::move(local_context)),
      log(target_storage->getLogger())
{
    source_storage = context->tryGetCnchWorkerResource()->getTable(StorageID{command.from_database, command.from_table});

    ingest_column_names = command.column_names;
    target_cloud_merge_tree = dynamic_cast<StorageCloudMergeTree *>(target_storage.get());
    if (!target_cloud_merge_tree)
        throw Exception("target table in worker is not CloudMeregTree", ErrorCodes::LOGICAL_ERROR);
    source_cloud_merge_tree = dynamic_cast<StorageCloudMergeTree *>(source_storage.get());

    if (!source_cloud_merge_tree)
        throw Exception("source table in worker is not CloudMeregTree", ErrorCodes::LOGICAL_ERROR);

    target_meta_data_ptr = target_cloud_merge_tree->getInMemoryMetadataPtr();
    source_meta_data_ptr = source_cloud_merge_tree->getInMemoryMetadataPtr();
    ordered_key_names = getOrderedKeys(command.key_names, *target_meta_data_ptr);
    partition_id = target_cloud_merge_tree->getPartitionIDFromQuery(command.partition, context);

    std::shared_ptr<Catalog::Catalog> catalog = context->getCnchCatalog();
    source_parts = getAndLoadPartsInWorker(
        *catalog, source_storage, *source_cloud_merge_tree, partition_id, context);
    LOG_DEBUG(log, "number of source parts: {}", source_parts.size());
    target_parts = getAndLoadPartsInWorker(
        *catalog, target_storage, *target_cloud_merge_tree, partition_id, context);
    LOG_DEBUG(log, "number of target parts: {}", target_parts.size());

    visible_source_parts = CnchPartsHelper::calcVisibleParts(source_parts, false, CnchPartsHelper::EnableLogging);
    LOG_DEBUG(log, "number of visible source parts: {}", visible_source_parts.size());
    visible_target_parts = CnchPartsHelper::calcVisibleParts(target_parts, false, CnchPartsHelper::EnableLogging);
    LOG_DEBUG(log, "number of visible target parts: {}", visible_target_parts.size());
}

Block IngestColumnBlockInputStream::readImpl()
{
    MemoryEfficientIngestColumn executor{*this};
    executor.execute();

    return Block{};
}

}
