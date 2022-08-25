#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{

class StorageSystemPersistentBGJobStatus : public shared_ptr_helper<StorageSystemPersistentBGJobStatus>,
				   public IStorageSystemOneBlock<StorageSystemPersistentBGJobStatus>
{
    friend struct shared_ptr_helper<StorageSystemPersistentBGJobStatus>;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

public:
    String getName() const override { return "SystemPersistentBackgroundJobStatus"; }
    static NamesAndTypesList getNamesAndTypes();

};

}
