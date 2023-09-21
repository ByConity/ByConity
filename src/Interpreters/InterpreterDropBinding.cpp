#include <Catalog/Catalog.h>
#include <Interpreters/InterpreterDropBinding.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Interpreters/SQLBinding/SQLBindingCatalog.h>
#include <Parsers/ASTSQLBinding.h>
#include <Interpreters/SQLBinding/SQLBindingUtils.h>

namespace DB
{
BlockIO InterpreterDropBinding::execute()
{
    const auto * drop = query_ptr->as<const ASTDropBinding>();
    if (!drop)
        throw Exception("Drop SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);

    bool is_re_binding = false;
    UUID pattern_uuid;
    if (drop->level == BindingLevel::SESSION)
    {
        auto session_binding_cache_manager = BindingCacheManager::getSessionBindingCacheManager(context);
        if (!session_binding_cache_manager)
            throw Exception("Can not get session binding cache manager", ErrorCodes::LOGICAL_ERROR);

        if (!drop->uuid.empty())
        {
            pattern_uuid = UUIDHelpers::toUUID(drop->uuid);
            if (session_binding_cache_manager->hasSqlBinding(pattern_uuid))
                session_binding_cache_manager->removeSqlBinding(pattern_uuid);
            else if (session_binding_cache_manager->hasReBinding(pattern_uuid))
                session_binding_cache_manager->removeReBinding(pattern_uuid);
            else
                throw Exception("Can not find binding uuid in SESSION bindings cache", ErrorCodes::LOGICAL_ERROR);
        }
        else if (!drop->pattern.empty())
        {
            pattern_uuid = SQLBindingUtils::getQueryHash(drop->pattern.data(), drop->pattern.data() + drop->pattern.size());
            if (session_binding_cache_manager->hasSqlBinding(pattern_uuid))
                session_binding_cache_manager->removeSqlBinding(pattern_uuid);
            else
                throw Exception("Can not find sql binding in SESSION bindings cache", ErrorCodes::LOGICAL_ERROR);
        }
        else if (!drop->re_expression.empty())
        {
            pattern_uuid
                = SQLBindingUtils::getReExpressionHash(drop->re_expression.data(), drop->re_expression.data() + drop->re_expression.size());
            if (session_binding_cache_manager->hasReBinding(pattern_uuid))
                session_binding_cache_manager->removeReBinding(pattern_uuid);
            else
                throw Exception("Can not find re_expression binding in SESSION bindings cache", ErrorCodes::LOGICAL_ERROR);
        }
        else
            throw Exception("Drop SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        auto global_binding_cache_manager = context->getGlobalBindingCacheManager();
        if (!global_binding_cache_manager)
            throw Exception("Can not get global binding cache manager", ErrorCodes::LOGICAL_ERROR);

        if (!drop->uuid.empty())
        {
            pattern_uuid = UUIDHelpers::toUUID(drop->uuid);
            if (global_binding_cache_manager->hasSqlBinding(pattern_uuid))
                global_binding_cache_manager->removeSqlBinding(pattern_uuid);
            else if (global_binding_cache_manager->hasReBinding(pattern_uuid))
            {
                global_binding_cache_manager->removeReBinding(pattern_uuid);
                is_re_binding = true;
            }
            else
                throw Exception("Can not find binding uuid in global bindings cache", ErrorCodes::LOGICAL_ERROR);
        }
        else if (!drop->pattern.empty())
        {
            pattern_uuid = SQLBindingUtils::getQueryHash(drop->pattern.data(), drop->pattern.data() + drop->pattern.size());
            if (global_binding_cache_manager->hasSqlBinding(pattern_uuid))
                global_binding_cache_manager->removeSqlBinding(pattern_uuid);
            else
                throw Exception("Can not find sql binding in global bindings cache", ErrorCodes::LOGICAL_ERROR);
        }
        else if (!drop->re_expression.empty())
        {
            pattern_uuid
                = SQLBindingUtils::getReExpressionHash(drop->re_expression.data(), drop->re_expression.data() + drop->re_expression.size());
            if (global_binding_cache_manager->hasReBinding(pattern_uuid))
                global_binding_cache_manager->removeReBinding(pattern_uuid);
            else
                throw Exception("Can not find re_expression binding in global bindings cache", ErrorCodes::LOGICAL_ERROR);
            is_re_binding = true;
        }
        else
            throw Exception("Drop SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);
    }

    // Update the cache of all servers
    if (drop->level == BindingLevel::GLOBAL)
    {
        BindingCatalogManager catalog(context);
        catalog.removeSQLBinding(pattern_uuid, is_re_binding);
        for (int i = 0; i < 3; ++i)
        {
            try
            {
                catalog.updateGlobalBindingCache(context);
            }
            catch (...)
            {
                if (i == 2)
                    throw Exception(
                        "The global binding is successfully removed from catalog and the current server cache, But can't sync to some other server's cache",
                        ErrorCodes::LOGICAL_ERROR);
                continue;
            }
            break;
        }
    }

    return {};
}

}
