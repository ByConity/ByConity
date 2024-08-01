#pragma once

#include <Catalog/IMetastore.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Catalog
{

class CatalogBackgroundTask
{

public:
    CatalogBackgroundTask(
        const ContextPtr & context_,
        const std::shared_ptr<IMetaStore> & metastore_,
        const String & name_space_);
    
    ~CatalogBackgroundTask();

    void execute();

private:

    void cleanStaleLargeKV();

    Poco::Logger * log = &Poco::Logger::get("CatalogBGTask");

    ContextPtr context;
    std::shared_ptr<IMetaStore> metastore;
    String name_space;

    BackgroundSchedulePool::TaskHolder task_holder;
};

}

}
