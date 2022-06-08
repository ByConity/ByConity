#pragma once
#include <Storages/IStorage_fwd.h>

namespace DB
{
class Context;
}

namespace DB::CnchTableHelper
{
/// Create storage object without database
StoragePtr createStorageFromQuery(const std::string & create_table_query, Context & context);

}
