#include <Catalog/Catalog.h>
#include <Interpreters/InterpreterDropBindingQuery.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Interpreters/SQLBinding/SQLBindingCatalog.h>
#include <Parsers/ASTSQLBinding.h>
#include <Interpreters/SQLBinding/SQLBindingUtils.h>

namespace DB
{
BlockIO InterpreterDropBindingQuery::execute()
{
    const auto * drop = query_ptr->as<const ASTDropBinding>();
    if (!drop)
        throw Exception("Drop SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);
    bool is_re_binding = false;
    UUID pattern_uuid;
    bool drop_success = true;

    auto process_binding_cache = [&](std::shared_ptr<BindingCacheManager> & binding_cache_manager) {
        if (!binding_cache_manager)
            throw Exception("Can not get binding cache manager", ErrorCodes::LOGICAL_ERROR);

        if (!drop->uuid.empty())
        {
            pattern_uuid = UUIDHelpers::toUUID(drop->uuid);
            if (binding_cache_manager->hasSqlBinding(pattern_uuid))
                binding_cache_manager->removeSqlBinding(pattern_uuid);
            else if (binding_cache_manager->hasReBinding(pattern_uuid))
            {
                binding_cache_manager->removeReBinding(pattern_uuid);
                is_re_binding = true;
            }
            else
            {
                if (!drop->if_exists)
                    throw Exception("Can not find binding uuid in bindings cache", ErrorCodes::LOGICAL_ERROR);
                drop_success = false;
            }
                
        }
        else if (!drop->pattern.empty())
        {
            pattern_uuid = SQLBindingUtils::getQueryASTHash(drop->pattern_ast);
            if (binding_cache_manager->hasSqlBinding(pattern_uuid))
                binding_cache_manager->removeSqlBinding(pattern_uuid);
            else
            {
                if (!drop->if_exists)
                    throw Exception("Can not find sql binding in bindings cache", ErrorCodes::LOGICAL_ERROR);
                drop_success = false;
            }
        }
        else if (!drop->re_expression.empty())
        {
            pattern_uuid
                = SQLBindingUtils::getReExpressionHash(drop->re_expression.data(), drop->re_expression.data() + drop->re_expression.size());
            if (binding_cache_manager->hasReBinding(pattern_uuid))
                binding_cache_manager->removeReBinding(pattern_uuid);
            else
            {
                if (!drop->if_exists)
                    throw Exception("Can not find re_expression binding in bindings cache", ErrorCodes::LOGICAL_ERROR);
                drop_success = false;
            }
            is_re_binding = true;
        }
        else
            throw Exception("Drop SQL Binding logical error", ErrorCodes::LOGICAL_ERROR);
    };

   
    if (drop->level == BindingLevel::SESSION)
    {
        auto session_binding_cache_manager = BindingCacheManager::getSessionBindingCacheManager(context);
        process_binding_cache(session_binding_cache_manager);
    }
    else
    {
        auto global_binding_cache_manager = context->getGlobalBindingCacheManager();
        process_binding_cache(global_binding_cache_manager);
    }

    // Update the cache of all servers
    if (drop->level == BindingLevel::GLOBAL && drop_success)
    {
        BindingCatalogManager catalog(context);
        catalog.removeSQLBinding(pattern_uuid, is_re_binding);
        try
        {
            catalog.updateGlobalBindingCache(context);
        }
        catch (...)
        {
            LOG_ERROR(log, "Drop SQL Binding succeeded, But some nodes failed to refresh sql binding cache.");
        }
    }

    return {};
}

}
