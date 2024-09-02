#pragma once
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IDatabase;

void attachMySQL(ContextMutablePtr context, IDatabase & mysql_database);

}
