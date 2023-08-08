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

static const Context* getQueryContext()
{
    /*
        It should be the global session context. Use thread-local query context instead.
    */
    if (CurrentThread::isInitialized())
        return CurrentThread::get().getQueryContext().get();
    return nullptr;
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
    auto query_context = getQueryContext();
    if (!query_context)
        return database_name;
    const String *tenant_id = &query_context->getTenantId();
    if (!tenant_id->empty() && !isInternalDatabaseName(database_name)
     && database_name.find(*tenant_id) != 0)
    {
        String result = *tenant_id;
        result += separator;
        result += database_name;
        return result;
    }
    return database_name;
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

// {tenant_id}.{original_database_name}
String getOriginalDatabaseName(const String & tenant_database_name)
{
    auto query_context = getQueryContext();
    if (!query_context)
        return tenant_database_name;

    const String *tenant_id = &query_context->getTenantId();
    if (tenant_id->empty())
        tenant_id = &query_context->getSettings().tenant_id.toString();
    if (!tenant_id->empty()) {
        auto size = tenant_id->size();
        if (tenant_database_name.size() > size + 1 && tenant_database_name[size] == '.'
            && memcmp(tenant_id->data(),tenant_database_name.data(), size) == 0)
            return tenant_database_name.substr(size + 1);
    }
    return tenant_database_name;
}

}
