#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

class StorageSystemVirtualWarehouseQueryQueue final : public shared_ptr_helper<StorageSystemVirtualWarehouseQueryQueue>,
                                      public IStorageSystemOneBlock<StorageSystemVirtualWarehouseQueryQueue>
{
    friend struct shared_ptr_helper<StorageSystemVirtualWarehouseQueryQueue>;

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemVirtualWarehouseQueryQueue"; }

    static NamesAndTypesList getNamesAndTypes();
};
}
