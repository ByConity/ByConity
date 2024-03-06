#include <optional>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/SystemLog.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_SESSION;
}

static const String catalog_delim = "$$";

static String getTenantId()
{
    String empty_result;
    if (!CurrentThread::isInitialized())
        return empty_result;
    auto context = CurrentThread::get().getQueryContext();
    if (context)
    {
        if (!context->getTenantId().empty())
            return context->getTenantId();
        else if (!context->getSettings().tenant_id.toString().empty())
            return context->getSettings().tenant_id.toString();
    }
    return CurrentThread::get().getTenantId();
}

String getCurrentCatalog()
{
    String empty_result;
    if (!CurrentThread::isInitialized())
        return empty_result;
    auto context = CurrentThread::get().getQueryContext();
    if (context)
    {
        if (!context->getCurrentCatalog().empty())
            return context->getCurrentCatalog();
        else if (!context->getSettings().default_catalog.toString().empty())
            return context->getSettings().default_catalog.toString();
    }
    return empty_result;
}

static String internal_databases[]
    = {DatabaseCatalog::SYSTEM_DATABASE, DatabaseCatalog::INFORMATION_SCHEMA, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE, DatabaseCatalog::TEMPORARY_DATABASE, CNCH_SYSTEM_LOG_DB_NAME, "default"};

static bool isInternalDatabaseName(const String & database_name)
{
    for (auto & db : internal_databases)
    {
        if (db == database_name)
            return true;
    }
    return false;
}

//Format pattern {tenant_id}.{database_name}
static String formatTenantDatabaseNameImpl(const String & database_name, char separator = '.')
{
    auto tenant_id = getTenantId();
    if (!tenant_id.empty() && !isInternalDatabaseName(database_name) &&
        (database_name.find(tenant_id) != 0 || database_name.size() == tenant_id.size() || database_name[tenant_id.size()] != separator))
    {
        String result = tenant_id;
        result += separator;
        result += database_name;
        return result;
    }
    return database_name;
}

//Format pattern {tenant_id}.{username}
static String formatTenantUserNameImpl(const String & user_name, char separator = '`')
{
    auto tenant_id = getTenantId();
    if (!tenant_id.empty() &&
        (user_name.find(tenant_id) != 0 || user_name.size() == tenant_id.size() || user_name[tenant_id.size()] != separator))
    {
        String result = tenant_id;
        result += separator;
        result += user_name;
        return result;
    }
    return user_name;
}

//Format pattern {tenant_id}.[catalog_name$$]{database_name}
String formatTenantDatabaseName(const String & database_name)
{
    auto catalog_name = getCurrentCatalog();
    // we don't need catalog name or database_name contains catalog info.
    if (catalog_name.empty() || database_name.find(catalog_delim) != String::npos)
    {
        return formatTenantDatabaseNameImpl(database_name);
    }
    else
    {
        return formatCatalogDatabaseName(database_name, catalog_name);
    }
}

String appendTenantIdOnly(const String & name)
{
    return formatTenantDatabaseNameImpl(name);
}

String formatTenantDatabaseNameWithTenantId(const String & database_name, const String & tenant_id, char separator)
{
    if (!tenant_id.empty() && !isInternalDatabaseName(database_name) && database_name.find(tenant_id) != 0)
    {
        String result = tenant_id;
        result += separator;
        result += database_name;
        return result;
    }
     return database_name;
}

//Format pattern {tenant_id}`{database_name}
String formatTenantConnectDefaultDatabaseName(const String & database_name)
{
     auto tenant_id = getTenantId();
    if (!tenant_id.empty())
    {
        auto pos = database_name.find(tenant_id);
        if (pos != 0 || database_name.size() == tenant_id.size() ||
            (database_name[tenant_id.size()] != '`' && database_name[tenant_id.size()] != '.'))
        {
            String result = tenant_id;
            result += '`';
            result += database_name;
            return result;
        }
        else if (pos == 0 && database_name.size() > tenant_id.size() && database_name[tenant_id.size()] == '.')
        {
            String result = database_name;
            result[tenant_id.size()] = '`';
            return result;
        }

    }
    return database_name;
}

//Format pattern {tenant_id}`{user_name}
String formatTenantConnectUserName(const String & user_name, bool is_force)
{
    auto tenant_id = getTenantId();
    if (!tenant_id.empty())
    {
        auto pos = user_name.find(tenant_id);
        if (is_force)
        {
            if (pos != 0 || user_name.size() == tenant_id.size() ||
                (user_name[tenant_id.size()] != '`' && user_name[tenant_id.size()] != '.'))
            {
                if (!is_force)
                    return user_name;
                String result = tenant_id;
                result += '`';
                result += user_name;
                return result;
            }
        }
        else if (pos == 0 && user_name.size() > tenant_id.size() && user_name[tenant_id.size()] == '.')
        {
            String result = user_name;
            result[tenant_id.size()] = '`';
            return result;
        }
    }
    return user_name;
}

//Format pattern {tenant_id}.{entity_name}
String formatTenantEntityName(const String & name)
{
    return formatTenantUserNameImpl(name, '.');
}

bool isTenantMatchedEntityName(const String & tenant_entity_name)
{
    auto tenant_id = getTenantId();
    if (!tenant_id.empty())
    {
        auto size = tenant_id.size();
        if (tenant_entity_name.size() > size + 1 && tenant_entity_name[size] == '.'
            && memcmp(tenant_id.data(),tenant_entity_name.data(), size) == 0)
            return true;
        return false;
    }
    return true;
}

String getOriginalEntityName(const String & tenant_entity_name)
{
    auto tenant_id = getTenantId();
    if (!tenant_id.empty()) {
        auto size = tenant_id.size();
        if (tenant_entity_name.size() > size + 1 && tenant_entity_name[size] == '.'
            && memcmp(tenant_id.data(),tenant_entity_name.data(), size) == 0)
            return tenant_entity_name.substr(size + 1);
    }
    return tenant_entity_name;
}

// {tenant_id}.{original_database_name}
String getOriginalDatabaseName(const String & tenant_database_name)
{
    return getOriginalDatabaseName(tenant_database_name, getTenantId());
}

String getOriginalDatabaseName(const String & tenant_database_name, const String & tenant_id)
{
    if (!tenant_id.empty())
    {
        auto size = tenant_id.size();
        if (tenant_database_name.size() > size + 1 && tenant_database_name[size] == '.'
            && memcmp(tenant_id.data(), tenant_database_name.data(), size) == 0)
            return tenant_database_name.substr(size + 1);
    }
    return tenant_database_name;
}

void pushTenantId(const String & tenant_id)
{
    CurrentThread::get().pushTenantId(tenant_id);
}

void popTenantId()
{
    CurrentThread::get().popTenantId();
}

bool catalogIsCnch(const String & catalog_name)
{
    return catalog_name.empty() || catalog_name == "cnch";
}

String formatCatalogDatabaseName(const String & database_name, const String catalog_name)
{
    if (isInternalDatabaseName(database_name) && database_name != "default")
        return database_name;

    // the hive also has "default" database;
    if (database_name == "default" && catalogIsCnch(catalog_name))
        return database_name;

    String original_db = getOriginalDatabaseName(database_name);
    String formatted_catalog = formatTenantDatabaseNameImpl(catalog_name);
    String result;
    result.reserve(database_name.size() + catalog_name.size() + catalog_delim.size());
    result.append(formatted_catalog).append(catalog_delim).append(database_name);
    return result;
}

std::tuple<std::optional<String>, std::optional<String>> getCatalogNameAndDatabaseName(const String & database_name)
{
    auto pos = database_name.find(catalog_delim);
    if (pos == String::npos)
        return {std::nullopt, {database_name}};
    return {{database_name.substr(0, pos)}, {database_name.substr(pos + catalog_delim.size())}};
}
}
