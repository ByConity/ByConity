#pragma once

#include <common/types.h>
#include <Interpreters/Context_fwd.h>
namespace DB
{

class Context;

String formatTenantDatabaseName(const String & database_name);

String formatTenantDefaultDatabaseName(const String & database_name);

String formatTenantConnectUserName(const String & user_name);

String getOriginalDatabaseName(const String & tenant_database_name);

void pushTenantId(const String &tenant_id);

void popTenantId();

}
