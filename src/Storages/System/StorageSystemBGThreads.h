#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
/** Implements `bg_threads` system table
  */
class StorageSystemBGThreads : public shared_ptr_helper<StorageSystemBGThreads>, // NOLINT
                               public IStorageSystemOneBlock<StorageSystemBGThreads>
{
public:
    std::string getName() const override { return "StorageSystemBGThreads"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
