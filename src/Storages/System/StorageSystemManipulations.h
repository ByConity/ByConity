#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemManipulations :
    public shared_ptr_helper<StorageSystemManipulations>,
    public IStorageSystemOneBlock<StorageSystemManipulations>
{
public:
    std::string getName() const override { return "SystemManipulations"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
