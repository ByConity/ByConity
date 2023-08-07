#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class StorageSystemCnchFilesystemLock : public shared_ptr_helper<StorageSystemCnchFilesystemLock>,
                                  public IStorageSystemOneBlock<StorageSystemCnchFilesystemLock>
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemTableFilesysLock";
    }

    static NamesAndTypesList getNamesAndTypes();
};

}
