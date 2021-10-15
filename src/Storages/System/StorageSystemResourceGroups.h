#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `resource_groups` system table
  */
class StorageSystemResourceGroups : public shared_ptr_helper<StorageSystemResourceGroups>, public IStorageSystemOneBlock<StorageSystemResourceGroups>
{
public:
    std::string getName() const override { return "SystemResourceGroups"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemResourceGroups>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
