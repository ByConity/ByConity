#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{

class StorageSystemGlobalGCManager : public shared_ptr_helper<StorageSystemGlobalGCManager>,
				   public IStorageSystemOneBlock<StorageSystemGlobalGCManager>
{
    friend struct shared_ptr_helper<StorageSystemGlobalGCManager>;

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    String getName() const override { return "StorageSystemGlobalGCManager"; }
    static NamesAndTypesList getNamesAndTypes();

};

}
