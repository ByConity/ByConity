#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
/** Implements `bg_task_statistics` system table
  */
class StorageSystemBGTaskStatistics : public shared_ptr_helper<StorageSystemBGTaskStatistics>, // NOLINT
                               public IStorageSystemOneBlock<StorageSystemBGTaskStatistics>
{
public:
    std::string getName() const override { return "StorageSystemBGTaskStatistics"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
