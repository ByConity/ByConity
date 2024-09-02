#include <Catalog/Catalog.h>
#include <Access/ContextAccess.h>
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

    if (drop->level == BindingLevel::GLOBAL)
    {
        AccessRightsElements access_rights_elements;
        access_rights_elements.emplace_back(AccessType::DROP_BINDING);
        context->checkAccess(access_rights_elements);
    }

    const auto & current_tenant_id = context->getTenantId();

    String binding_tenant_id;
    auto process_binding_cache = [&](std::shared_ptr<BindingCacheManager> & binding_cache_manager) {
        if (!binding_cache_manager)
            throw Exception("Can not get binding cache manager", ErrorCodes::LOGICAL_ERROR);

        if (!drop->uuid.empty())
        {
            pattern_uuid = UUIDHelpers::toUUID(drop->uuid);
            binding_tenant_id = binding_cache_manager->getTenantID(pattern_uuid);
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
            }
                
        }
        else if (!drop->pattern.empty())
        {
            pattern_uuid = SQLBindingUtils::getQueryASTHash(drop->pattern_ast, current_tenant_id);
            binding_cache_manager->removeSqlBinding(pattern_uuid, !drop->if_exists);
        }
        else if (!drop->re_expression.empty())
        {
            pattern_uuid
                = SQLBindingUtils::getReExpressionHash(drop->re_expression.data(), drop->re_expression.data() + drop->re_expression.size(), current_tenant_id);
            binding_cache_manager->removeReBinding(pattern_uuid, !drop->if_exists);
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
    if (drop->level == BindingLevel::GLOBAL)
    {
        BindingCatalogManager catalog(context);
        binding_tenant_id = current_tenant_id.empty() ? binding_tenant_id : current_tenant_id;
        catalog.removeSQLBinding(pattern_uuid, binding_tenant_id, is_re_binding);
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
