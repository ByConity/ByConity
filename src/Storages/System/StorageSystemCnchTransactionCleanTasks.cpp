#include <mutex>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemCnchTransactionCleanTasks.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

namespace DB
{

NamesAndTypesList StorageSystemCnchTransactionCleanTasks::getNamesAndTypes()
{
    return {
        {"id", std::make_shared<DataTypeUInt64>()},
        {"txn_status", std::make_shared<DataTypeString>()},
        {"priority", std::make_shared<DataTypeInt32>()},
        {"elapsed_milliseconds", std::make_shared<DataTypeUInt64>()},
        {"undo_part_size", std::make_shared<DataTypeUInt32>()},
    };
}

void StorageSystemCnchTransactionCleanTasks::fillData(MutableColumns& res_columns,
    ContextPtr context, const SelectQueryInfo&) const
{
    if (context->getServerType() != ServerType::cnch_server)
    {
        return;
    }

    auto& txn_cleaner = context->getCnchTransactionCoordinator().getTxnCleaner();
    {
        auto lock = txn_cleaner.getLock();
        const auto& tasks = txn_cleaner.getAllTasksUnLocked();
        for (const auto& [txn_id, task] : tasks)
        {
            int i = 0;
            res_columns[i++]->insert(static_cast<UInt64>(task.txn_id));
            res_columns[i++]->insert(txnStatusToString(task.txn_status));
            res_columns[i++]->insert(static_cast<Int32>(task.priority));
            res_columns[i++]->insert(task.elapsed());
            res_columns[i++]->insert(task.undo_size.load(std::memory_order_relaxed));
        }
    }
}

}
