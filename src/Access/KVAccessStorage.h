#pragma once

#include <Common/ThreadPool.h>
#include <boost/container/flat_set.hpp>
#include <Access/IAccessStorage.h>
#include <Catalog/Catalog.h>
#include <chrono>
#include <Interpreters/Context_fwd.h>
#include <Core/BackgroundSchedulePool.h>


namespace DB
{
class Context;
/// Loads and saves access entities on KV.
class KVAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "key-value storage";

    KVAccessStorage(const ContextPtr & context);
    // ~KVAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    // RBAC TODO: Enable and add details about catalog here
    // String getStorageParamsJSON() const override;

    bool isReadOnly() const { return false; }
    void stopBgJob();
    void onAccessEntityChanged(EntityType type, const String & tenanted_name) const;

private:
    std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(EntityType type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID & id) const override;
    bool canInsertImpl(const AccessEntityPtr & entity) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;

    void clear();
    void updateExpiredEntries();

    void insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, Notifications & notifications);
    void removeNoLock(const UUID & id, Notifications & notifications);
    void updateNoLock(const UUID & id, const UpdateFunc & update_func, Notifications & notifications);

    struct Entry
    {
        UUID id;
        String name;
        EntityType type;
        UInt64 commit_time;
        UInt64 creation_time;
        mutable AccessEntityPtr entity; /// is guaranteed to be loaded by findImpl
        mutable std::list<OnChangedHandler> handlers_by_id;

        Entry()
        {
            creation_time = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        }

        bool isExpired(UInt64 expire_time_ms)
        {
            UInt64 time_now = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            return (time_now - creation_time) >= expire_time_ms;
        }
    };

    UUID addEntry(EntityType type, const AccessEntityModel & entity_model, Notifications & notifications) const;

    void prepareNotifications(const UUID & id, const Entry & entry, bool remove, Notifications & notifications) const;

    mutable std::unordered_map<UUID, Entry> entries_by_id;
    mutable std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(EntityType::MAX)];
    mutable std::list<OnChangedHandler> handlers_by_type[static_cast<size_t>(EntityType::MAX)];
    mutable std::mutex mutex;
    Catalog::CatalogPtr catalog;
    BackgroundSchedulePool::TaskHolder task;
    UInt64 ttl_ms;
};
}
