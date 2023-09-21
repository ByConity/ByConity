#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{
class Context;

class StorageSystemQueryQueue final : public shared_ptr_helper<StorageSystemQueryQueue>,
                                      public IStorageSystemOneBlock<StorageSystemQueryQueue>
{
    friend struct shared_ptr_helper<StorageSystemQueryQueue>;

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemQueryQueue"; }

    static NamesAndTypesList getNamesAndTypes();
};
}
