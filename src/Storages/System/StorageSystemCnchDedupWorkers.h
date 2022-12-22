#pragma once

#include <CloudServices/CnchBGThreadsMap.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
class StorageSystemCnchDedupWorkers : public shared_ptr_helper<StorageSystemCnchDedupWorkers>,
                                  public IStorageSystemOneBlock<StorageSystemCnchDedupWorkers>
{
public:
    explicit StorageSystemCnchDedupWorkers(const StorageID & table_id_) : IStorageSystemOneBlock(table_id_) { }

    std::string getName() const override { return "SystemDedupWorkers"; }

    static NamesAndTypesList getNamesAndTypes();

    ColumnsDescription getColumnsAndAlias();

protected:
    const FormatSettings format_settings;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    void fillDataOnServer(
        MutableColumns & res_columns,
        const ColumnPtr & needed_uuids,
        const std::map<UUID, StoragePtr> & tables,
        ContextPtr context,
        const UUIDToBGThreads & uuid_to_threads) const;

    void fillDataOnWorker(
        MutableColumns & res_columns, const ColumnPtr & needed_uuids, const std::map<UUID, StoragePtr> & tables, ContextPtr context) const;
};
}
