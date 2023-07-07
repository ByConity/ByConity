#pragma once

#include <common/types.h>
namespace DB
{

class Context;

String formatTenantDatabaseName(const String & database_name);

String formatTenantDefaultDatabaseName(const String & database_name);

String getOriginalDatabaseName(const String & tenant_database_name);

}
