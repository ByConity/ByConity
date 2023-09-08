#include <Storages/System/StorageSystemIOSchedulers.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Array.h>
#include <IO/Scheduler/IOScheduler.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_MANAGER_ERROR;
}

NamesAndTypesList StorageSystemIOSchedulers::getNamesAndTypes()
{
    return {
        {"status", std::make_shared<DataTypeString>()}
    };
}

void StorageSystemIOSchedulers::fillData(MutableColumns & res_columns, [[maybe_unused]]const ContextPtr context, const SelectQueryInfo & /*query_info*/) const
{
    IO::Scheduler::IOSchedulerSet& instance = IO::Scheduler::IOSchedulerSet::instance();
    if (!instance.enabled()) {
        return;
    }

    const std::vector<std::unique_ptr<IO::Scheduler::IOScheduler>>& schedulers =
        instance.rawSchedulers();
    Poco::JSON::Object scheduler_status_obj;
    scheduler_status_obj.set("scheduler_count", schedulers.size());
    Poco::JSON::Array scheduler_status;
    Poco::JSON::Parser parser;
    for (size_t i = 0; i < schedulers.size(); ++i) {
        parser.reset();
        scheduler_status.add(parser.parse(schedulers.at(i)->status()));
    }
    scheduler_status_obj.set("scheduler_status", scheduler_status);
    std::ostringstream os;
    scheduler_status_obj.stringify(os);
    res_columns[0]->insert(os.str());
}

}
