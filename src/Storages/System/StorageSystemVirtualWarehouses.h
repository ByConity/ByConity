#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemVirtualWarehouses: public shared_ptr_helper<StorageSystemVirtualWarehouses>, public IStorageSystemOneBlock<StorageSystemVirtualWarehouses>
{
public:
    std::string getName() const override
    {
        return "SystemVirtualWarehouseInfo";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const ContextPtr context, const SelectQueryInfo &) const override;
};

}
