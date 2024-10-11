#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class NexusFS;
class Context;


/** Implements system table asynchronous_metrics, which allows to get values of periodically (asynchronously) updated metrics.
  */
class StorageSystemNexusFS final : public shared_ptr_helper<StorageSystemNexusFS>,
    public IStorageSystemOneBlock<StorageSystemNexusFS>
{
    friend struct shared_ptr_helper<StorageSystemNexusFS>;
public:
    std::string getName() const override { return "SystemNexusFS"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
#if defined(ARCADIA_BUILD)
    StorageSystemNexusFS(const String & name_,)
    : StorageSystemNexusFS(StorageID{"system", name_})
    {
    }
#endif
    explicit StorageSystemNexusFS(const StorageID & table_id_);

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
