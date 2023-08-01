#include <chrono>
#include <memory>
#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Interpreters/SQLBinding/SQLBindingCatalog.h>
#include <Parsers/ASTSerDerHelper.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

void BindingCacheManager::initializeGlobalBinding(ContextMutablePtr & context)
{
    if (!context->getGlobalBindingCacheManager())
    {
        auto manager_instance = std::make_unique<BindingCacheManager>();
        context->setGlobalBindingCacheManager(std::move(manager_instance));
    }

    auto manager_instance = context->getGlobalBindingCacheManager();

    if (manager_instance->sql_binding_cache && manager_instance->re_binding_cache)
    {
        LOG_WARNING(&Poco::Logger::get("BindingCacheManager"), "Global BindingCacheManager already initialized");
        return;
    }

    manager_instance->initialize(SQLBindingCacheCacheConfig::max_cache_size);
    updateGlobalBindingsFromCatalog(context);
}

void BindingCacheManager::initializeSessionBinding()
{
    if (sql_binding_cache && re_binding_cache)
    {
        LOG_WARNING(&Poco::Logger::get("BindingCacheManager"), "Sesion BindingCacheManager already initialized");
        return;
    }

    initialize(SQLBindingCacheCacheConfig::max_cache_size);
}

void BindingCacheManager::initialize(UInt64 max_size)
{
    if (!sql_binding_cache)
        sql_binding_cache = std::make_unique<CacheType>(max_size);

    if (!re_binding_cache)
        re_binding_cache = std::make_unique<CacheType>(max_size);
}

std::shared_ptr<BindingCacheManager> BindingCacheManager::getSessionBindingCacheManager(const ContextMutablePtr & query_context)
{
    if (query_context->hasSessionContext())
        return query_context->getSessionContext()->getSessionBindingCacheManager();
    else
        return query_context->getSessionBindingCacheManager();
}

BindingCacheManager::CacheType & BindingCacheManager::getSqlCacheInstance()
{
    if (!sql_binding_cache)
        throw Exception("SQLBinding gloabl sql_cache has to be initialized", ErrorCodes::LOGICAL_ERROR);
    return *sql_binding_cache;
}

BindingCacheManager::CacheType & BindingCacheManager::getReCacheInstance()
{
    if (!re_binding_cache)
        throw Exception("SQLBinding global re_cache has to be initialized", ErrorCodes::LOGICAL_ERROR);
    return *re_binding_cache;
}

void BindingCacheManager::addReBinding(const UUID & id, const SQLBindingObject & binding)
{
    if (re_keys_mutex.try_lock_for(std::chrono::milliseconds(1000)))
    {
        re_bindings_order_list.remove(id);
        re_bindings_order_list.emplace_back(id);
        re_binding_cache->add(id, binding);
        re_keys_mutex.unlock();
    }
    else
        throw Exception("get ReBinding cache local failed", ErrorCodes::TIMEOUT_EXCEEDED);
}

void BindingCacheManager::removeReBinding(const UUID & id)
{
    if (re_keys_mutex.try_lock_for(std::chrono::milliseconds(1000)))
    {
        re_bindings_order_list.remove(id);
        re_binding_cache->remove(id);
        re_keys_mutex.unlock();
    }
    else
        throw Exception("get ReBinding cache local failed", ErrorCodes::TIMEOUT_EXCEEDED);
}

void BindingCacheManager::updateGlobalBindingsFromCatalog(const ContextPtr & context)
{
    if (!context->getGlobalBindingCacheManager())
        throw Exception("Catalog has to be initialized", ErrorCodes::LOGICAL_ERROR);

    BindingCatalogManager catalog(context);
    auto global_bindings = catalog.getSQLBindings();
    std::sort(global_bindings.begin(), global_bindings.end(), [](const SQLBindingItemPtr & binding1, const SQLBindingItemPtr & binding2) {
        return binding1->timestamp < binding2->timestamp;
    });

    auto manager_instance = context->getGlobalBindingCacheManager();
    auto & global_sql_cache = manager_instance->getSqlCacheInstance();
    global_sql_cache.clear();
    manager_instance->getReCacheInstance().clear();
    for (const auto & binding : global_bindings)
    {
        if (!binding->pattern.empty() && !binding->serialized_ast.empty())
        {
            ReadBufferFromString ast_buffer(binding->serialized_ast);
            auto ast = deserializeAST(ast_buffer);
            if (binding->is_regular_expression)
            {
                auto re_ptr = std::make_shared<boost::regex>(binding->pattern);
                manager_instance->addReBinding(binding->uuid, {binding->pattern, nullptr, ast, re_ptr});
            }
            else
                global_sql_cache.add(binding->uuid, {binding->pattern, ast, nullptr, nullptr});
        }
    }
    manager_instance->setTimeStamp(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

std::list<UUID> BindingCacheManager::getReKeys()
{
    std::list<UUID> re_keys;
    if (re_keys_mutex.try_lock_shared())
    {
        re_keys = re_bindings_order_list;
        re_keys_mutex.unlock_shared();
    }
    return re_keys;
}

bool BindingCacheManager::hasReBinding(const UUID & id)
{
    bool has_re_binding = false;
    if (re_keys_mutex.try_lock_shared())
    {
        has_re_binding = re_binding_cache->has(id);
        re_keys_mutex.unlock_shared();
    }
    return has_re_binding;
}

}
