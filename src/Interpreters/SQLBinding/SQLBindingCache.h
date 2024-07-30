#pragma once

#include <memory>
#include <mutex>
#include <Interpreters/Context.h>
#include <boost/regex.hpp>
#include <Poco/LRUCache.h>
#include <Poco/RWLock.h>
#include <Common/HashTable/Hash.h>

namespace DB
{

namespace SQLBindingCacheCacheConfig
{
    constexpr UInt64 max_cache_size = 1024UL;
}

struct SQLBindingObject
{
    String pattern;
    String tenant_id;
    ASTPtr target_ast; // use for sql binding
    ASTPtr settings; // use for re binding
    std::shared_ptr<boost::regex> re; // use for re binding
};

class BindingCacheManager
{
public:
    using CacheType = Poco::LRUCache<UUID, SQLBindingObject>;

    void initialize(UInt64 max_size);

    static void initializeGlobalBinding(ContextMutablePtr & context);

    void initializeSessionBinding();

    CacheType & getSqlCacheInstance();

    CacheType & getReCacheInstance();

    bool hasSqlBinding(const UUID & id) { return sql_binding_cache->has(id); }

    void removeSqlBinding(const UUID & id, bool throw_if_not_exists = false);

    void addSqlBinding(const UUID & id, const SQLBindingObject & binding, bool throw_if_exists, bool or_replace);

    bool hasReBinding(const UUID & id);

    void removeReBinding(const UUID & id, bool throw_if_not_exists = false);

    void addReBinding(const UUID & id, const SQLBindingObject & binding, bool throw_if_exists, bool or_replace);

    static std::shared_ptr<BindingCacheManager> getSessionBindingCacheManager(const ContextMutablePtr & query_context);

    std::list<UUID> getReKeys();

    long getTimeStamp() const { return global_update_time_stamp; }

    void setTimeStamp(long time_stamp) { global_update_time_stamp = time_stamp; }

    static void updateGlobalBindingsFromCatalog(const ContextPtr & context);

    bool isReBindingEmpty() { return re_binding_cache->size() == 0; }

    bool isSqlBindingEmpty() { return sql_binding_cache->size() == 0; }

    String getTenantID(const UUID & id);
private:
    std::unique_ptr<CacheType> sql_binding_cache;
    std::unique_ptr<CacheType> re_binding_cache;
    std::list<UUID> re_bindings_order_list;
    long global_update_time_stamp = 0;
    std::shared_timed_mutex re_keys_mutex;
};
using BindingCacheManagerPtr = std::shared_ptr<BindingCacheManager>;

}
