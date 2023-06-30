#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/formatTenantDatabaseName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_SESSION;
}

static String getTenantId()
{
    String empty_result;
    if (!CurrentThread::isInitialized())
        return empty_result;
    const Context *context = CurrentThread::get().getQueryContext().get();
    if (context)
    {
        if (!context->getTenantId().empty())
            return context->getTenantId();
        else if (!context->getSettings().tenant_id.toString().empty())
            return context->getSettings().tenant_id.toString();
    }
    return CurrentThread::get().getTenantId();
}

static String internal_databases[] = {
    DatabaseCatalog::SYSTEM_DATABASE,
    DatabaseCatalog::TEMPORARY_DATABASE,
    "default"
};

static bool isInternalDatabaseName(const String& database_name)
{
    for(auto& db : internal_databases)
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
    if (!tenant_id.empty() && !isInternalDatabaseName(database_name)
     && database_name.find(tenant_id) != 0)
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
    if (!tenant_id.empty() && user_name.find(tenant_id) != 0)
    {
        String result = tenant_id;
        result += separator;
        result += user_name;
        return result;
    }
    return user_name;
}

//Format pattern {tenant_id}.{database_name}
String formatTenantDatabaseName(const String & database_name)
{
    return formatTenantDatabaseNameImpl(database_name);
}

//Format pattern {tenant_id}`{database_name}
String formatTenantDefaultDatabaseName(const String & database_name)
{
    return formatTenantDatabaseNameImpl(database_name, '`');
}

//Format pattern {tenant_id}`{user_name}
String formatTenantConnectUserName(const String & user_name)
{
    return formatTenantUserNameImpl(user_name, '`');
}

//Format pattern {tenant_id}_{entity prefix}
String formatTenantEntityPrefix(const String & prefix)
{
    return formatTenantUserNameImpl(prefix, '.');
}

// {tenant_id}.{original_database_name}
String getOriginalDatabaseName(const String & tenant_database_name)
{
    auto tenant_id = getTenantId();
    if (!tenant_id.empty()) {
        auto size = tenant_id.size();
        if (tenant_database_name.size() > size + 1 && tenant_database_name[size] == '.'
            && memcmp(tenant_id.data(),tenant_database_name.data(), size) == 0)
            return tenant_database_name.substr(size + 1);
    }
    return tenant_database_name;
}

void pushTenantId(const String &tenant_id)
{
    CurrentThread::get().pushTenantId(tenant_id);
}

void popTenantId()
{
    CurrentThread::get().popTenantId();
}

}
