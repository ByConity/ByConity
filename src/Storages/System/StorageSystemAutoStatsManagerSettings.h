#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{

class Context;
/** implements system table "settings", which allows to get information about the current settings.
  */
class StorageSystemAutoStatsManagerSettings final : public shared_ptr_helper<StorageSystemAutoStatsManagerSettings>,
                                                    public IStorageSystemOneBlock<StorageSystemAutoStatsManagerSettings>
{
    friend struct shared_ptr_helper<StorageSystemAutoStatsManagerSettings>;

public:
    std::string getName() const override
    {
        return "SystemAutoStatsManagerSettings";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
