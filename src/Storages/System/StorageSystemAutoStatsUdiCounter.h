#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{

class Context;

class StorageSystemAutoStatsUdiCounter final : public shared_ptr_helper<StorageSystemAutoStatsUdiCounter>,
                                               public IStorageSystemOneBlock<StorageSystemAutoStatsUdiCounter>
{
    friend struct shared_ptr_helper<StorageSystemAutoStatsUdiCounter>;

public:
    std::string getName() const override { return "SystemAutoStatsUdiCounter"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
