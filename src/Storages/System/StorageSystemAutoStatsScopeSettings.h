#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>


namespace DB
{

class Context;
/** implements system table "settings", which allows to get information about the current settings.
  */
class StorageSystemAutoStatsScopeSettings final : public shared_ptr_helper<StorageSystemAutoStatsScopeSettings>,
                                                    public IStorageSystemOneBlock<StorageSystemAutoStatsScopeSettings>
{
    friend struct shared_ptr_helper<StorageSystemAutoStatsScopeSettings>;

public:
    std::string getName() const override
    {
        return "SystemAutoStatsScopeSettings";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
