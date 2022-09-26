#pragma once
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
namespace DB
{
namespace DaemonManager
{
UUID getUUIDFromCreateQuery(const DB::Protos::DataModelDictionary & d);
void fixCatalogMetaData(ContextPtr context, Poco::Logger * log);
}
}
