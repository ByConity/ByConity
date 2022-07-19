#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemCnchManipulations :
    public shared_ptr_helper<StorageSystemCnchManipulations>,
    public IStorageSystemOneBlock<StorageSystemCnchManipulations>
{
public:
    std::string getName() const override { return "SystemCnchManipulations"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
