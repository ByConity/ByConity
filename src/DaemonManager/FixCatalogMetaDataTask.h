#pragma once
#include <Interpreters/Context_fwd.h>
namespace DB
{
namespace DaemonManager
{
UUID getUUIDFromCreateQuery(const DB::Protos::DataModelDictionary & d);
void fixCatalogMetaData(ContextPtr context, Logger * log);
}
}
