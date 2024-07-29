#pragma once

#include <Storages/IStorage.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

class StorageSystemCnchManifestList : public shared_ptr_helper<StorageSystemCnchManifestList>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemCnchManifestList>;
public:
    std::string getName() const override { return "SystemCnchManifestList"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        const size_t max_block_size,
        const unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }

 protected:
    StorageSystemCnchManifestList(const StorageID & table_id_);
};

}
