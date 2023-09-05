#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class Context;

class StorageSystemQueryCache final : public shared_ptr_helper<StorageSystemQueryCache>, public IStorageSystemOneBlock<StorageSystemQueryCache>
{
    friend struct shared_ptr_helper<StorageSystemQueryCache>;
public:
    std::string getName() const override { return "SystemQueryCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;
};

}
