#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class Context;
/** implements system table "settings", which allows to get information about the current settings.
  */
class StorageSystemStatisticsTableView final : public shared_ptr_helper<StorageSystemStatisticsTableView>,
                                                    public IStorageSystemOneBlock<StorageSystemStatisticsTableView>
{
    friend struct shared_ptr_helper<StorageSystemStatisticsTableView>;

public:
    std::string getName() const override
    {
        return "SystemStatisticsTableView";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
