#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class StorageSystemWorkerGroups : public shared_ptr_helper<StorageSystemWorkerGroups>, public IStorageSystemOneBlock<StorageSystemWorkerGroups>
{
public:
    std::string getName() const override
    {
        return "SystemWorkerGroups";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const ContextPtr context, const SelectQueryInfo &) const override;
};

}
