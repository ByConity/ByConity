#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class Context;

/// Implements the `unique_logs` system table, which allows you to view the manifest logs of unique tables.
class StorageSystemUniqueLogs : public shared_ptr_helper<StorageSystemUniqueLogs>, public IStorageSystemOneBlock<StorageSystemUniqueLogs>
{
    friend struct shared_ptr_helper<StorageSystemUniqueLogs>;
public:
    std::string getName() const override { return "SystemUniqueLogs"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
