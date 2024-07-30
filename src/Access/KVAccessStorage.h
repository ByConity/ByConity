#pragma once

#include <Common/ThreadPool.h>
#include <boost/container/flat_set.hpp>
#include <Access/IAccessEntity.h>
#include <Access/IAccessStorage.h>
#include <Catalog/Catalog.h>
#include <chrono>
#include <Interpreters/Context_fwd.h>
#include <Core/BackgroundSchedulePool.h>


namespace DB
{
class Context;
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;
/// Loads and saves access entities on KV.
class KVAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "key-value storage";

    explicit KVAccessStorage(const ContextPtr & context);
    ~KVAccessStorage() override = default;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    // RBAC TODO: Enable and add details about catalog here
    // String getStorageParamsJSON() const override;

    bool isReadOnly() const { return false; }

    static constexpr int SHARD_CNT = 256;

    static int getShard(const UUID & id)
    {
        return id.toUnderType().items[0] % SHARD_CNT;
    }

    static int getShard(const String & name)
    {
        //We do not need to use the whole string to evaluate the hash result
        //Usually the names shares the same prefix, so we shard from the back.
        const auto start = name.cend() - std::min(4, static_cast<int>(name.size()));
        return std::accumulate(start, name.cend(), 0) % SHARD_CNT;
    }

    void loadEntities(EntityType type, const std::unordered_set<UUID> & ids) const;

private:
    std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(EntityType type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    bool canInsertImpl(const AccessEntityPtr & entity) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;

    struct Entry
    {
        UUID id;
        String name;
        EntityType type;
        UInt64 commit_time;
        mutable AccessEntityPtr entity; /// is guaranteed to be loaded by findImpl
        mutable std::list<OnChangedHandler> handlers_by_id;
        AccessEntityModel entity_model;
    };

    UUID updateCache(EntityType type, const AccessEntityModel & entity_model, Notifications * notifications, const AccessEntityPtr & entity = nullptr) const;
    UUID updateCache(EntityType type, const AccessEntityModel & entity_model, const AccessEntityPtr & entity = nullptr) const;

    void prepareNotifications(const UUID & id, const Entry & entry, bool remove, Notifications & notifications) const;

    void dropEntry(const Entry &e) const;
    bool getEntityModelByNameAndType(const String & name, EntityType type, AccessEntityModel &model) const;
    bool getEntryByUUID(const UUID &uuid, Entry &entry) const;
    bool getEntryByNameAndType(const String &name, EntityType type, Entry &entry) const;
    struct Entry * getEntryReferenceByUUID(const UUID &uuid) const TSA_REQUIRES(mutex);

    mutable std::unordered_map<UUID, Entry> entries_by_id[SHARD_CNT] TSA_GUARDED_BY(mutex);
    mutable std::unordered_map<String, Entry *> entries_by_name_and_type[static_cast<size_t>(EntityType::MAX)][SHARD_CNT] TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;

    mutable std::list<OnChangedHandler> handlers_by_type[static_cast<size_t>(EntityType::MAX)] TSA_GUARDED_BY(hdl_mutex);
    mutable std::mutex hdl_mutex;

    Catalog::CatalogPtr catalog;
};
}
