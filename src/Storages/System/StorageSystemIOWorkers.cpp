#include <Storages/System/StorageSystemIOWorkers.h>

#include <IO/Scheduler/IOScheduler.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_MANAGER_ERROR;
}

NamesAndTypesList StorageSystemIOWorkers::getNamesAndTypes()
{
    return {
        {"status", std::make_shared<DataTypeString>()}
    };
}

void StorageSystemIOWorkers::fillData(MutableColumns & res_columns, const ContextPtr context, const SelectQueryInfo & /*query_info*/) const
{
    IO::Scheduler::IOSchedulerSet& instance = IO::Scheduler::IOSchedulerSet::instance();
    if (!instance.enabled()) {
        return;
    }

    IO::Scheduler::IOWorkerPool* worker_pool = instance.workerPool();
    if (worker_pool == nullptr) {
        return;
    }

    res_columns[0]->insert(worker_pool->status());
}

}
