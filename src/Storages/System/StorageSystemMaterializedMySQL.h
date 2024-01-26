#pragma once
#include <Storages/StorageSnapshot.h>
#include "config_core.h"
#if USE_MYSQL

#include <Storages/IStorage.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class StorageSystemMaterializedMySQL : public shared_ptr_helper<StorageSystemMaterializedMySQL>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemMaterializedMySQL>;
public:
    std::string getName() const override { return "SystemMaterializedMySQL"; }

    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    bool isSystemStorage() const override { return true; }

protected:
    explicit StorageSystemMaterializedMySQL(const StorageID & table_id_);
};

}
#endif
