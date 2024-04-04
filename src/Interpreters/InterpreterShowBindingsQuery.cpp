#include <string>
#include <Catalog/Catalog.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <Interpreters/InterpreterShowBindingsQuery.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Parsers/ASTSQLBinding.h>
#include <Interpreters/SQLBinding/SQLBindingUtils.h>
#include <Core/Types.h>
#include <Core/UUID.h>

namespace DB
{
BlockIO InterpreterShowBindingsQuery::execute()
{
    const auto * show = query_ptr->as<const ASTShowBindings>();
    if (!show)
        throw Exception("Show SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);

    size_t cnt = 0;
    Block block;
    std::ostringstream out;
    auto session_binding_cache_manager = BindingCacheManager::getSessionBindingCacheManager(context);
    auto & session_sql_cache = session_binding_cache_manager->getSqlCacheInstance();
    for (const auto & key : session_sql_cache.getAllKeys())
    {
        cnt++;
        auto sql_binding_ptr = session_sql_cache.get(key);
        out << SQLBindingUtils::getShowBindingsHeader(cnt) << "Session Binding UUID: " << UUIDHelpers::UUIDToString(key) << "\n"
            << "Pattern:\n"
            << sql_binding_ptr->pattern << ";\n"
            << "Bound Query:\n"
            << SQLBindingUtils::getASTStr(sql_binding_ptr->target_ast) << "\n";
    }

    auto & session_re_cache = session_binding_cache_manager->getReCacheInstance();
    for (const auto & key : session_binding_cache_manager->getReKeys())
    {
        if (!session_re_cache.has(key))
            continue;
        cnt++;
        const auto & sql_binding_ptr = session_re_cache.get(key);
        out << SQLBindingUtils::getShowBindingsHeader(cnt) << "Session Binding UUID: " << UUIDHelpers::UUIDToString(key) << "\n"
            << "Pattern: " << sql_binding_ptr->pattern << "\n"
            << "Settings: " << SQLBindingUtils::getASTStr(sql_binding_ptr->settings) << "\n";
    }

    auto global_binding_cache_manager = context->getGlobalBindingCacheManager();
    auto time_stamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if ((time_stamp - global_binding_cache_manager->getTimeStamp()) > static_cast<long>(context->getSettingsRef().global_bindings_update_time))
    {
        try
        {
            BindingCacheManager::updateGlobalBindingsFromCatalog(context);
        }
        catch (...)
        {
            LOG_WARNING(&Poco::Logger::get("SQL Binding"), "Update Global Bindings Failed");
        }
    }

    auto & global_sql_cache = global_binding_cache_manager->getSqlCacheInstance();
    for (const auto & key : global_sql_cache.getAllKeys())
    {
        cnt++;
        auto sql_binding_ptr = global_sql_cache.get(key);
        out << SQLBindingUtils::getShowBindingsHeader(cnt) << "Global Binding UUID: " << UUIDHelpers::UUIDToString(key) << "\n"
            << "Pattern:\n"
            << sql_binding_ptr->pattern << ";\n"
            << "Bound Query:\n"
            << SQLBindingUtils::getASTStr(sql_binding_ptr->target_ast) << "\n";
    }

    auto & global_re_cache = global_binding_cache_manager->getReCacheInstance();
    for (const auto & key : global_binding_cache_manager->getReKeys())
    {
        if (!global_re_cache.has(key))
            continue ;
        cnt++;
        auto sql_binding_ptr = global_re_cache.get(key);
        out << SQLBindingUtils::getShowBindingsHeader(cnt) << "Global Binding UUID: " << UUIDHelpers::UUIDToString(key)  << "\n"
            << "Pattern: " << sql_binding_ptr->pattern << "\n"
            << "Settings: " << SQLBindingUtils::getASTStr(sql_binding_ptr->settings) << "\n";
    }

    BlockIO res;
    MutableColumnPtr binding_column = ColumnString::create();
    std::istringstream ss(out.str());
    std::string line;
    while (std::getline(ss, line))
    {
        binding_column->insert(std::move(line));
    }

    res.in = std::make_shared<OneBlockInputStream>(Block{{std::move(binding_column), std::make_shared<DataTypeString>(), "Bindings"}});
    return res;
}

}
