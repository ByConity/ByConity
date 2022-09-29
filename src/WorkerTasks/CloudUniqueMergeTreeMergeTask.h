#pragma once

#include <memory>
#include <CloudServices/commitCnchParts.h>
#include <Core/Types.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <WorkerTasks/ManipulationTask.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <common/logger_useful.h>

namespace DB
{
namespace CatalogService
{
    class Catalog;
}

class MergeTreeDataMerger;
class StorageCloudMergeTree;

class CloudUniqueMergeTreeMergeTask : public std::enable_shared_from_this<CloudUniqueMergeTreeMergeTask>, public ManipulationTask
{
public:
    CloudUniqueMergeTreeMergeTask(StorageCloudMergeTree & storage_, ManipulationTaskParams params_, ContextPtr context_);

    void executeImpl() override;

private:
    DeleteBitmapMetaPtrVector
    getDeleteBitmapMetas(Catalog::Catalog & catalog, const IMergeTreeDataPartsVector & parts, TxnTimestamp ts);

    void updateDeleteBitmap(Catalog::Catalog & catalog, const MergeTreeDataMerger & merger, DeleteBitmapPtr & out_bitmap);

    StorageCloudMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    CnchDataWriter cnch_writer;
    String partition_id;

    DeleteBitmapMetaPtrVector prev_bitmap_metas;
    DeleteBitmapMetaPtrVector curr_bitmap_metas;
    ImmutableDeleteBitmapVector prev_bitmaps;
    ImmutableDeleteBitmapVector curr_bitmaps;
};

} // namespace DB
