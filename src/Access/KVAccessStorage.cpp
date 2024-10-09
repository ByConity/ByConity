/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Access/KVAccessStorage.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/RowPolicy.h>
#include <Access/Quota.h>
#include <Access/SettingsProfile.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ParserCreateRoleQuery.h>
#include <Parsers/ParserCreateRowPolicyQuery.h>
#include <Parsers/ParserCreateQuotaQuery.h>
#include <Parsers/ParserCreateSettingsProfileQuery.h>
#include <Parsers/ParserGrantQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Interpreters/InterpreterCreateRoleQuery.h>
#include <Interpreters/InterpreterCreateRowPolicyQuery.h>
#include <Interpreters/InterpreterCreateQuotaQuery.h>
#include <Interpreters/InterpreterCreateSettingsProfileQuery.h>
#include <Interpreters/InterpreterGrantQuery.h>
#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <fstream>
#include <thread>
#include <chrono>
#include <Protos/RPCHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_ACCESS_ENTITY_DEFINITION;
    extern const int CONCURRENT_RBAC_UPDATE;
}


namespace
{
    using EntityType = IAccessStorage::EntityType;
    using EntityTypeInfo = IAccessStorage::EntityTypeInfo;

    /// Special parser for the 'ATTACH access entity' queries.
    class ParserAttachAccessEntity : public IParserBase
    {
    protected:
        const char * getName() const override { return "ATTACH access entity query"; }

        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
        {
            ParserCreateUserQuery create_user_p;
            ParserCreateRoleQuery create_role_p;
            ParserCreateRowPolicyQuery create_policy_p(ParserSettings::CLICKHOUSE);
            ParserCreateQuotaQuery create_quota_p;
            ParserCreateSettingsProfileQuery create_profile_p;
            ParserGrantQuery grant_p;

            create_user_p.useAttachMode();
            create_role_p.useAttachMode();
            create_policy_p.useAttachMode();
            create_quota_p.useAttachMode();
            create_profile_p.useAttachMode();
            grant_p.useAttachMode();

            return create_user_p.parse(pos, node, expected) || create_role_p.parse(pos, node, expected)
                || create_policy_p.parse(pos, node, expected) || create_quota_p.parse(pos, node, expected)
                || create_profile_p.parse(pos, node, expected) || grant_p.parse(pos, node, expected);
        }
    };


    /// Reads a file containing ATTACH queries and then parses it to build an access entity.
    AccessEntityPtr convertFromSqlToEntity(const String & create_sql, const String & sensitive_sql)
    {

        /// Parse the create sql.
        ASTs queries;
        auto parse_sql = [&](const String & sql, bool sensitive_mode) {
            ParserAttachAccessEntity parser;
            const char * begin = sql.data(); /// begin of current query
            const char * pos = begin; /// parser moves pos from begin to the end of current query
            const char * end = begin + sql.size();
            while (pos < end)
            {
                ASTPtr ast = parseQueryAndMovePosition(parser, pos, end, "", true, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                /* ignore ATTACH queries in sensitive mode */
                if (!sensitive_mode || ast->as<ASTGrantQuery>())
                    queries.emplace_back(ast);
                while (isWhitespaceASCII(*pos) || *pos == ';')
                    ++pos;
            }
        };

        parse_sql(create_sql, false);
        parse_sql(sensitive_sql, true);

        /// Interpret the AST to build an access entity.
        std::shared_ptr<User> user;
        std::shared_ptr<Role> role;
        std::shared_ptr<RowPolicy> policy;
        std::shared_ptr<Quota> quota;
        std::shared_ptr<SettingsProfile> profile;
        AccessEntityPtr res;
        bool sensitive_tenant = !sensitive_sql.empty();

        for (const auto & query : queries)
        {
            if (auto * create_user_query = query->as<ASTCreateUserQuery>())
            {
                if (res)
                    throw Exception("Two access entities in one sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = user = std::make_unique<User>();
                InterpreterCreateUserQuery::updateUserFromQuery(*user, *create_user_query);
            }
            else if (auto * create_role_query = query->as<ASTCreateRoleQuery>())
            {
                if (res)
                    throw Exception("Two access entities in one sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = role = std::make_unique<Role>();
                InterpreterCreateRoleQuery::updateRoleFromQuery(*role, *create_role_query);
            }
            else if (auto * create_policy_query = query->as<ASTCreateRowPolicyQuery>())
            {
                if (res)
                    throw Exception("Two access entities in one sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = policy = std::make_unique<RowPolicy>();
                InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(*policy, *create_policy_query);
            }
            else if (auto * create_quota_query = query->as<ASTCreateQuotaQuery>())
            {
                if (res)
                    throw Exception("Two access entities are attached in the same sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = quota = std::make_unique<Quota>();
                InterpreterCreateQuotaQuery::updateQuotaFromQuery(*quota, *create_quota_query);
            }
            else if (auto * create_profile_query = query->as<ASTCreateSettingsProfileQuery>())
            {
                if (res)
                    throw Exception("Two access entities are attached in the same sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                res = profile = std::make_unique<SettingsProfile>();
                InterpreterCreateSettingsProfileQuery::updateSettingsProfileFromQuery(*profile, *create_profile_query);
            }
            else if (auto * grant_query = query->as<ASTGrantQuery>())
            {
                /* sensitive permissions were serialized first */
                if (grant_query->is_sensitive)
                    sensitive_tenant = true;

                if (!user && !role)
                    throw Exception("A user or role should be attached before grant in sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                if (user)
                    InterpreterGrantQuery::updateUserFromQuery(*user, *grant_query, sensitive_tenant);
                else
                    InterpreterGrantQuery::updateRoleFromQuery(*role, *grant_query, sensitive_tenant);
            }
            else
                throw Exception("No interpreter found for query " + query->getID(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
        }

        if (!res)
            throw Exception("No access entities attached in sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);

        return res;
    }


    /// Writes ATTACH queries for building a specified access entity to a file.
    String convertFromEntityToSql(const IAccessEntity & entity, bool sensitive_mode)
    {
        /// Build list of ATTACH queries.
        ASTs queries;
        queries.push_back(InterpreterShowCreateAccessEntityQuery::getAttachQuery(entity));
        if ((entity.getType() == EntityType::USER) || (entity.getType() == EntityType::ROLE))
        {
            ASTs grants = InterpreterShowGrantsQuery::getAttachGrantQueries(entity, sensitive_mode);
            if (grants.empty() && sensitive_mode)
                return {};
            boost::range::push_back(queries, grants);
        }

        /// Serialize the list of ATTACH queries to a string.
        WriteBufferFromOwnString buf;
        for (const ASTPtr & query : queries)
        {
            formatAST(*query, buf, false, true);
            buf.write(";\n", 2);
        }

        return buf.str();
    }

    class ConcurrentAccessGuard {
    public:
        ConcurrentAccessGuard & operator=(const ConcurrentAccessGuard &) = delete;
        ConcurrentAccessGuard(const ConcurrentAccessGuard &) = delete;
        ConcurrentAccessGuard() = delete;
        explicit ConcurrentAccessGuard(const UUID &uuid)
        {
            {
                std::scoped_lock lock(map_mtx);
                current_mtx = &access[uuid];
            }
            current_mtx->lock();
        }

        ~ConcurrentAccessGuard()
        {
            current_mtx->unlock();
        }

    private:
        static std::unordered_map<UUID, std::mutex> access TSA_GUARDED_BY(map_mtx);
        static std::mutex map_mtx;
        std::mutex * current_mtx;
    };

    std::unordered_map<UUID, std::mutex> ConcurrentAccessGuard::access;
    std::mutex ConcurrentAccessGuard::map_mtx;
}

KVAccessStorage::KVAccessStorage(const ContextPtr & context)
    : IAccessStorage(STORAGE_TYPE)
    , catalog(context->getCnchCatalog())
{
    // Preload entities only on server nodes. Worker nodes loads entities on need only basis (eg. when perfer_cnch_catalog = 1)
    if (context->getServerType() == ServerType::cnch_server)
    {
        for (auto type : collections::range(EntityType::MAX))
            findAllImpl(type);
    }
}

void KVAccessStorage::dropEntry(const Entry &e) const
{
    auto name_shard = getShard(e.name);
    auto id_shard = getShard(e.id);
    auto & name_map = entries_by_name_and_type[static_cast<size_t>(e.type)][name_shard];
    auto & uuid_map = entries_by_id[id_shard];

    {
        std::lock_guard lock{mutex};
        name_map.erase(e.name);
        uuid_map.erase(e.id);
    }

    Notifications notifications;
    prepareNotifications(e.id, e, true, notifications);
    notify(notifications);
}

bool KVAccessStorage::getEntryByUUID(const UUID &uuid, Entry &entry) const
{
    auto id_shard = getShard(uuid);
    const auto & uuid_map = entries_by_id[id_shard];

    std::lock_guard lock{mutex};
    const auto & it = uuid_map.find(uuid);

    if (it == uuid_map.end())
        return false;

    entry = it->second;
    return true;
}

struct KVAccessStorage::Entry * KVAccessStorage::getEntryReferenceByUUID(const UUID &uuid) const
{
    auto id_shard = getShard(uuid);
    auto & uuid_map = entries_by_id[id_shard];

    auto it = uuid_map.find(uuid);

    if (it == uuid_map.end())
        return nullptr;

    return &it->second;
}

bool KVAccessStorage::getEntityModelByNameAndType(const String & name, EntityType type, AccessEntityModel &model) const
{
    auto name_shard = getShard(name);
    const auto & name_map = entries_by_name_and_type[static_cast<size_t>(type)][name_shard];

    std::lock_guard lock{mutex};
    const auto & it = name_map.find(name);

    if (it == name_map.end())
        return false;

    model = it->second->entity_model;
    return true;
}

bool KVAccessStorage::getEntryByNameAndType(const String &name, EntityType type, Entry &entry) const
{
    auto name_shard = getShard(name);
    const auto & name_map = entries_by_name_and_type[static_cast<size_t>(type)][name_shard];

    std::lock_guard lock{mutex};
    const auto & it = name_map.find(name);

    if (it == name_map.end())
        return false;

    entry = *it->second;
    return true;
}

UUID KVAccessStorage::updateCache(EntityType type, const AccessEntityModel & entity_model, const AccessEntityPtr & entity) const
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    return updateCache(type, entity_model, &notifications, entity);
}

UUID KVAccessStorage::updateCache(EntityType type, const AccessEntityModel & entity_model, Notifications * notifications, const AccessEntityPtr & entity_) const
{
    UUID uuid = RPCHelpers::createUUID(entity_model.uuid());
    auto id_shard = getShard(uuid);
    auto & uuid_map = entries_by_id[id_shard];

    std::unique_lock lock{mutex};

    if (uuid_map.contains(uuid))
    {
        /* memory entry matches KV store*/
        if (uuid_map[uuid].commit_time == entity_model.commit_time())
            return uuid;
    }

    lock.unlock();
    auto name_shard = getShard(entity_model.name());
    auto & name_map = entries_by_name_and_type[static_cast<size_t>(type)][name_shard];
    const auto & entity = entity_ ? entity_ : convertFromSqlToEntity(entity_model.create_sql(), entity_model.sensitive_sql());

    lock.lock();
    auto & entry = uuid_map[uuid];
    entry.type = type;
    entry.name = entity_model.name();
    entry.id = uuid;
    entry.commit_time = entity_model.commit_time();
    entry.entity = entity;
    entry.entity_model = entity_model;
    name_map[entry.name] = &entry;

    auto entry_copy = entry;
    lock.unlock();

    if (notifications)
        prepareNotifications(uuid, entry_copy, false, *notifications);
    return uuid;
}


// Always get entity from KV to ensure that we have the most updated Entity at all times
std::optional<UUID> KVAccessStorage::findImpl(EntityType type, const String & name) const
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    auto entity_model = catalog->tryGetAccessEntity(type, name);

    if (!entity_model)
    {
        Entry entry;

        if (getEntryByNameAndType(name, type, entry))
            dropEntry(entry);

        return std::nullopt;
    }

    return updateCache(type, *entity_model, &notifications);
}


std::vector<UUID> KVAccessStorage::findAllImpl(EntityType type) const
{
    auto entity_models = catalog->getAllAccessEntities(type);
    std::vector<UUID> res;
    res.reserve(entity_models.size());

    for (const auto & entity_model : entity_models)
        res.emplace_back(updateCache(type, entity_model, nullptr));

    return res;
}

bool KVAccessStorage::existsImpl(const UUID & id) const
{
    {
        auto id_shard = getShard(id);
        auto & entries_map = entries_by_id[id_shard];

        std::lock_guard lock{mutex};
        if (entries_map.contains(id))
            return true;
    }
    if (catalog->tryGetAccessEntityName((id)))
        return true;

    return false;
}


AccessEntityPtr KVAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    auto id_shard = getShard(id);
    auto & entries_map = entries_by_id[id_shard];

    std::lock_guard lock{mutex};
    auto it = entries_map.find(id);
    if (it == entries_map.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return nullptr;
    }

    const auto & entry = it->second;
    return entry.entity;
}


bool KVAccessStorage::canInsertImpl(const AccessEntityPtr &) const
{
    return true;
}


UUID KVAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists)
{
    const String & name = new_entity->getName();
    EntityType type = new_entity->getType();
    AccessEntityModel old_entity_model;
    bool replace;
    UUID uuid;

    if (getEntityModelByNameAndType(name, type, old_entity_model))
    {
        if (!replace_if_exists)
            throwNameCollisionCannotInsert(type, name);

        uuid = RPCHelpers::createUUID(old_entity_model.uuid());
        replace = true;
    }
    else
    {
        uuid = UUIDHelpers::generateV4();
        replace = false;
    }


    ConcurrentAccessGuard guard(uuid);
    if (replace)
    {
        if (getEntityModelByNameAndType(name, type, old_entity_model))
        {
            if (!replace_if_exists)
                throwNameCollisionCannotInsert(type, name);
        }
    }

    AccessEntityModel new_entity_model;
    RPCHelpers::fillUUID(uuid, *(new_entity_model.mutable_uuid()));
    new_entity_model.set_name(name);
    new_entity_model.set_create_sql(convertFromEntityToSql(*new_entity, false));
    auto sensitive_sql = convertFromEntityToSql(*new_entity, true);
    if (!sensitive_sql.empty())
        new_entity_model.set_sensitive_sql(sensitive_sql);
    catalog->putAccessEntity(type, new_entity_model, old_entity_model, replace_if_exists);

    updateCache(type, new_entity_model, new_entity);
    return uuid;
}

void KVAccessStorage::removeImpl(const UUID & uuid)
{
    Entry e;

    ConcurrentAccessGuard guard(uuid);
    if (!getEntryByUUID(uuid, e))
        throwNotFound(uuid);

    catalog->dropAccessEntity(e.type, e.id, e.name);

    dropEntry(e);
}

void KVAccessStorage::updateImpl(const UUID & uuid, const UpdateFunc & update_func)
{
    bool name_changed;
    Entry old_entry;

    ConcurrentAccessGuard guard(uuid);
    if (!getEntryByUUID(uuid, old_entry))
        throwNotFound(uuid);

    AccessEntityPtr new_entity = update_func(old_entry.entity);

    const String new_name = new_entity->getName();
    name_changed = new_name != old_entry.name;

    if (!new_entity->isTypeOf(old_entry.type))
        throwBadCast(uuid, new_entity->getType(), new_name, old_entry.type);

    if (name_changed)
    {
        auto name_shard = getShard(new_name);
        const auto & name_map = entries_by_name_and_type[static_cast<size_t>(old_entry.type)][name_shard];
        std::lock_guard lock{mutex};
        if (name_map.contains(new_name))
            throwNameCollisionCannotRename(old_entry.type, old_entry.name, new_name);
    }

    if (*new_entity == *old_entry.entity && !name_changed)
        return;

    /* update KV */
    AccessEntityModel new_entity_model;
    RPCHelpers::fillUUID(uuid, *(new_entity_model.mutable_uuid()));
    new_entity_model.set_name(new_name);
    new_entity_model.set_create_sql(convertFromEntityToSql(*new_entity, false));
    auto sensitive_sql = convertFromEntityToSql(*new_entity, true);
    if (!sensitive_sql.empty())
        new_entity_model.set_sensitive_sql(sensitive_sql);
    catalog->putAccessEntity(old_entry.type, new_entity_model, old_entry.entity_model);

    Notifications notifications;
    int old_name_shard = -1;
    int new_name_shard = -1;
    if (name_changed)
    {
        old_name_shard = getShard(old_entry.name);
        new_name_shard = getShard(new_name);
    }

    std::unique_lock lock{mutex};
    Entry *entry = getEntryReferenceByUUID(uuid);
    if (entry)
    {
        if (new_entity_model.commit_time() < entry->commit_time)
            throw Exception("Concurrent rbac update, model had been overwritten by another server",  ErrorCodes::CONCURRENT_RBAC_UPDATE);

        entry->entity = std::move(new_entity);
        entry->commit_time = new_entity_model.commit_time();
        entry->entity_model = std::move(new_entity_model);

        if (name_changed)
        {
            chassert(old_name_shard != -1 && new_name_shard != -1);
            entry->name = new_name;
            auto & old_name_map = entries_by_name_and_type[static_cast<size_t>(old_entry.type)][old_name_shard];
            auto & new_name_map = entries_by_name_and_type[static_cast<size_t>(old_entry.type)][new_name_shard];

            old_name_map.erase(old_entry.name);
            new_name_map[new_name] = entry;
        }

        prepareNotifications(uuid, *entry, false, notifications);
    }
    else
    {
        throw Exception("Concurrent rbac update, possibly dropped from another server",  ErrorCodes::CONCURRENT_RBAC_UPDATE);
    }
    lock.unlock();

    notify(notifications);
}

void KVAccessStorage::prepareNotifications(const UUID & id, const Entry & entry, bool remove, Notifications & notifications) const
{
    if (!remove && !entry.entity)
        return;

    const AccessEntityPtr entity = remove ? nullptr : entry.entity;
    for (const auto & handler : entry.handlers_by_id)
        notifications.push_back({handler, id, entity});

    const auto & handlers = handlers_by_type[static_cast<size_t>(entry.type)];
    if (handlers.empty())
        return;

    std::lock_guard lock{hdl_mutex};
    for (const auto & handler : handlers)
        notifications.push_back({handler, id, entity});
}


scope_guard KVAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    auto id_shard = getShard(id);
    auto & uuid_map = entries_by_id[id_shard];

    std::lock_guard lock{mutex};
    auto it = uuid_map.find(id);
    if (it == uuid_map.end())
        return {};

    const Entry & entry = it->second;
    auto handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler);

    return [this, id, handler_it, &uuid_map]
    {
        std::lock_guard lock2{mutex};
        auto it2 = uuid_map.find(id);
        if (it2 != uuid_map.end())
        {
            const Entry & entry2 = it2->second;
            entry2.handlers_by_id.erase(handler_it);
        }
    };
}

scope_guard KVAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    auto & handlers = handlers_by_type[static_cast<size_t>(type)];

    std::lock_guard lock{hdl_mutex};
    handlers.push_back(handler);
    auto handler_it = std::prev(handlers.end());

    return [this, type, handler_it]
    {
        auto & handlers2 = handlers_by_type[static_cast<size_t>(type)];

        std::lock_guard lock2{hdl_mutex};
        handlers2.erase(handler_it);
    };
}

bool KVAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    auto id_shard = getShard(id);
    auto & uuid_map = entries_by_id[id_shard];

    std::lock_guard lock{mutex};
    auto it = uuid_map.find(id);
    if (it != uuid_map.end())
    {
        const Entry & entry = it->second;
        return !entry.handlers_by_id.empty();
    }
    return false;
}

bool KVAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];

    std::lock_guard lock{hdl_mutex};
    return !handlers.empty();
}

void KVAccessStorage::loadEntities(EntityType type, const std::unordered_set<UUID> & ids) const
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    auto entity_models = catalog->getEntities(type, ids);

    for (const auto & entity_model : entity_models)
        updateCache(type, entity_model, &notifications);
}

}
