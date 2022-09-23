#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class Context;
class StorageSystemLockMap : public shared_ptr_helper<StorageSystemLockMap>,
                                 public IStorageSystemOneBlock<StorageSystemLockMap>
{
public:
    std::string getName() const override { return "SystemLockManager"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
