#pragma once

#include <common/types.h>
#include <Interpreters/Context_fwd.h>
namespace DB
{

class Context;

// database_name -> tenant_id.[catalog$$].database_name
String formatTenantDatabaseName(const String & database_name);

// name -> tenant_id.name
// no catalog information will be attached.
String appendTenantIdOnly(const String & name);

String formatTenantConnectDefaultDatabaseName(const String & database_name);

String formatTenantDatabaseNameWithTenantId(const String & database_name, const String & tenant_id, char separator = '.');

String formatTenantConnectUserName(const String & user_name, bool is_force = false);

String formatTenantEntityName(const String & name);

String getOriginalEntityName(const String & tenant_entity_name);

bool isTenantMatchedEntityName(const String & tenant_entity_name);

String getOriginalDatabaseName(const String & tenant_database_name);

String getOriginalDatabaseName(const String & tenant_database_name, const String & tenant_id);

void pushTenantId(const String &tenant_id);

void popTenantId();

// get catalog information from query context.
String getCurrentCatalog();


// tenant_id.db -> tenant_id.catalog$$db
// db -> catalog$$db
String formatCatalogDatabaseName(const String & database_name, const String catalog_name);

// tenant_id.db -> teannt_id.db
// tenant_id.catalog$$db -> teanant_id.catalog, db
// catalog$$db -> catalog, db
std::tuple<std::optional<String>, std::optional<String>> getCatalogNameAndDatabaseName(const String & database_name);
}
