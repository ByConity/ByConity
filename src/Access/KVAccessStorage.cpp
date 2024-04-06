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
    AccessEntityPtr convertFromSqlToEntity(const String & create_sql)
    {

        /// Parse the create sql.
        ASTs queries;
        ParserAttachAccessEntity parser;
        const char * begin = create_sql.data(); /// begin of current query
        const char * pos = begin; /// parser moves pos from begin to the end of current query
        const char * end = begin + create_sql.size();
        while (pos < end)
        {
            queries.emplace_back(parseQueryAndMovePosition(parser, pos, end, "", true, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH));
            while (isWhitespaceASCII(*pos) || *pos == ';')
                ++pos;
        }

        /// Interpret the AST to build an access entity.
        std::shared_ptr<User> user;
        std::shared_ptr<Role> role;
        std::shared_ptr<RowPolicy> policy;
        std::shared_ptr<Quota> quota;
        std::shared_ptr<SettingsProfile> profile;
        AccessEntityPtr res;

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
                if (!user && !role)
                    throw Exception("A user or role should be attached before grant in sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
                if (user)
                    InterpreterGrantQuery::updateUserFromQuery(*user, *grant_query);
                else
                    InterpreterGrantQuery::updateRoleFromQuery(*role, *grant_query);
            }
            else
                throw Exception("No interpreter found for query " + query->getID(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
        }

        if (!res)
            throw Exception("No access entities attached in sql: " + create_sql, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);

        return res;
    }


    /// Writes ATTACH queries for building a specified access entity to a file.
    String convertFromEntityToSql(const IAccessEntity & entity)
    {
        /// Build list of ATTACH queries.
        ASTs queries;
        queries.push_back(InterpreterShowCreateAccessEntityQuery::getAttachQuery(entity));
        if ((entity.getType() == EntityType::USER) || (entity.getType() == EntityType::ROLE))
            boost::range::push_back(queries, InterpreterShowGrantsQuery::getAttachGrantQueries(entity));

        /// Serialize the list of ATTACH queries to a string.
        WriteBufferFromOwnString buf;
        for (const ASTPtr & query : queries)
        {
            formatAST(*query, buf, false, true);
            buf.write(";\n", 2);
        }
        return buf.str();
    }
}

KVAccessStorage::KVAccessStorage(const ContextPtr & context) 
    : IAccessStorage(STORAGE_TYPE)
    , catalog(context->getCnchCatalog())
    , ttl_ms(context->getSettingsRef().access_entity_ttl.value.totalMilliseconds())
{
    // Init all entities from KV
    clear();
    // Preload entities only on server nodes. Worker nodes loads entities on need only basis (eg. when perfer_cnch_catalog = 1)
    if (context->getServerType() == ServerType::cnch_server)
    {
        for (auto type : collections::range(EntityType::MAX))
            findAllImpl(type);
    }
}

KVAccessStorage::~KVAccessStorage()
{
    clear();
}

void KVAccessStorage::clear()
{
    bool first = false;
    do
    {
        if (!first)
            std::this_thread::sleep_for(std::chrono::seconds(1));
        first = true;
        std::lock_guard lock{mutex};
        if (prepared_entities > 0)
            continue;
        for (auto & entry_map :entries_by_id)
            entry_map.clear();
        for (auto type : collections::range(EntityType::MAX))
            // collections::range(MAX_CONDITION_TYPE) give us a range of [0, MAX_CONDITION_TYPE) 
            // coverity[overrun-local]
            for (auto & entry_name_map: entries_by_name_and_type[static_cast<size_t>(type)])
                entry_name_map.clear();
        break;
    } while(true);
}


UUID KVAccessStorage::addEntry(EntityType type, const AccessEntityModel & entity_model, Notifications & notifications, AccessEntityPtr access_ptr) const
{
    UUID uuid = RPCHelpers::createUUID(entity_model.uuid());
    auto & entry = entries_by_id[getShard(uuid)][uuid];
    if (!entry.name.empty())
    {
        entry.refreshTime();
        if(entry.commit_time == entity_model.commit_time())
            return uuid;
        if (entry.prepare_verison != entry.commit_version)
        {
            // We do not deal with concurrent insert/update conflict.
            throw Exception("Concurrent rbac update: " + entity_model.name(), ErrorCodes::CONCURRENT_RBAC_UPDATE);
        }
    }
    entry.type = type;
    entry.name = entity_model.name();
    entry.id = uuid;
    entry.commit_time = entity_model.commit_time();
    entry.entity = access_ptr ? access_ptr : convertFromSqlToEntity(entity_model.create_sql());
    entry.entity_model = entity_model;
    auto & entries_by_name_map = entries_by_name_and_type[static_cast<size_t>(type)][getShard(entry.name)];
    entries_by_name_map[entry.name] = &entry;
    prepareNotifications(uuid, entry, false, notifications);
    return uuid;
}

// Always get entity from KV to ensure that we have the most updated Entity at all times
std::optional<UUID> KVAccessStorage::findImpl(EntityType type, const String & name) const
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    auto entity_model = catalog->tryGetAccessEntity(type, name);
    std::lock_guard lock{mutex};
    if (entity_model)
        return addEntry(type, *entity_model, notifications);
    return {};
}



void KVAccessStorage::onAccessEntityChanged(EntityType type, const String & name) const
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    int retry = 4;
    std::optional<AccessEntityModel> entity_model;
    do
    {
        try
        {
            entity_model = catalog->tryGetAccessEntity(type, name);
            AccessEntityPtr access_entity = nullptr;
            if (entity_model)
                access_entity = convertFromSqlToEntity(entity_model->create_sql());
            std::lock_guard lock{mutex};
            if (entity_model)
                addEntry(type, *entity_model, notifications, access_entity);
            else
            {
                auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)][getShard(name)];
                auto it = entries_by_name.find(name);
                if (it != entries_by_name.end())
                {
                    Entry & entry = *(it->second);
                    if (entry.prepare_verison != entry.commit_version)
                        return;
                    prepareNotifications(entry.id, entry, true, notifications);
                    entries_by_name.erase(it);
                    entries_by_id[getShard(entry.id)].erase(entry.id);
                }
            }
            break;
        }
        catch (...)
        {
            //May be an concurrent update is executing; or catalog visiting failure
            //silently wait for it to finish or recovery
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    } while(retry-- > 0);
}



std::vector<UUID> KVAccessStorage::findAllImpl(EntityType type) const
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    auto entity_models = catalog->getAllAccessEntities(type);
    std::vector<UUID> res;
    res.reserve(entity_models.size());
    std::lock_guard lock{mutex};
    for (const auto & entity_model : entity_models)
        res.emplace_back(addEntry(type, entity_model, notifications));
    return res;
}

bool KVAccessStorage::existsImpl(const UUID & id) const
{
    {
        auto & entries_map = entries_by_id[getShard(id)];
        std::lock_guard lock{mutex};
        if (entries_map.count(id))
            return true;
    }
    if (catalog->tryGetAccessEntityName((id)))
        return true;
        
    return false;
}


AccessEntityPtr KVAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    auto & entries_map = entries_by_id[getShard(id)];
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
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    Int64 current_version = -1;
    const String & name = new_entity->getName();
    EntityType type = new_entity->getType();
    AccessEntityModel old_entity_model;
    Entry *entry_ptr = nullptr;
    bool rollback = true;
    auto & entries_by_name_map = entries_by_name_and_type[static_cast<size_t>(type)][getShard(name)];
    {
        std::lock_guard lock{mutex};
        auto old_it = entries_by_name_map.find(name);
        if (old_it != entries_by_name_map.end())
        {
            if (!replace_if_exists)
                throwNameCollisionCannotInsert(type, name);
            entry_ptr = old_it->second;
            if (entry_ptr->prepare_verison != entry_ptr->commit_version)
            {
                // We do not deal with concurrent creating conflict.
                throw Exception("Concurrent rbac insert: " + name, ErrorCodes::CONCURRENT_RBAC_UPDATE);
            }
            ++entry_ptr->prepare_verison;
            current_version = entry_ptr->prepare_verison;
            old_entity_model = entry_ptr->entity_model;
        }
    }
    ++prepared_entities;
    SCOPE_EXIT({
        //We always gurantee this is a valid pointer.
        if (rollback)
        {
            std::lock_guard lock{mutex};
            --prepared_entities;
            if (entry_ptr)
                --entry_ptr->prepare_verison;
        }
    });

    UUID id = UUIDHelpers::generateV4();
    auto & entries_id_map = entries_by_id[getShard(id)];
    AccessEntityModel new_entity_model;
    RPCHelpers::fillUUID(id, *(new_entity_model.mutable_uuid()));
    new_entity_model.set_name(name);
    new_entity_model.set_create_sql(convertFromEntityToSql(*new_entity));
    catalog->putAccessEntity(type, new_entity_model, old_entity_model, replace_if_exists);

    {
        std::lock_guard lock{mutex};
        /// Add entry.
        auto it = entries_by_name_map.find(name);
        if (it != entries_by_name_map.end() && !entry_ptr) 
        {
            // We do not deal with concurrent creating conflict.
            // Here the insert maybe sucessful, but we could not easily know the accurate update order.
            // So we reject to update the local cache here.
            throw Exception("Concurrent rbac insert: " + name, ErrorCodes::CONCURRENT_RBAC_UPDATE);
        }

        rollback = false;
        auto & entry = entries_id_map[id];
        entry.refreshTime();
        entry.id = id;
        entry.type = type;
        entry.name = name;
        entry.commit_time = new_entity_model.commit_time();
        entry.entity = new_entity;
        entry.entity_model = new_entity_model;
        entries_by_name_map[entry.name] = &entry;
        prepareNotifications(id, entry, false, notifications);
        if (entry_ptr)
        {
            ++entry_ptr->commit_version;
        }
        --prepared_entities;
    }
    return id;
}

void KVAccessStorage::removeImpl(const UUID & id)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    Int64 current_version = -1;
    EntityType type;
    String name;
    Entry* entry_ptr = nullptr;
    auto & entries_map = entries_by_id[getShard(id)];
    bool rollback = true;
    std::unordered_map<UUID, Entry>::iterator it;
    {
        std::lock_guard lock{mutex};
        it = entries_map.find(id);
        if (it == entries_map.end())
            throwNotFound(id);

        Entry & entry = it->second;
        entry_ptr = &entry;
        if (entry_ptr->prepare_verison != entry_ptr->commit_version)
        {
            // We do not deal with concurrent creating conflict.
            throw Exception("Concurrent rbac remove: " + name, ErrorCodes::CONCURRENT_RBAC_UPDATE);
        }
        type = entry.type;
        name = entry.name;
        ++entry_ptr->prepare_verison;
        current_version = entry_ptr->prepare_verison;
        prepared_entities++;
    }

    SCOPE_EXIT({
        if (rollback)
        {
            //Rollback
            std::lock_guard lock{mutex};
            --prepared_entities;
            if (entry_ptr)
                --entry_ptr->prepare_verison;
        }
    });

    catalog->dropAccessEntity(type, id, name);
    auto & entries_name_map = entries_by_name_and_type[static_cast<size_t>(type)][getShard(name)];

    rollback = false;
    std::lock_guard lock{mutex};
    /// Do removing.
    prepareNotifications(id, *entry_ptr, true, notifications);
    entries_name_map.erase(name);
    entries_map.erase(it);
    //entry_ptr is already become invalid, so ++entry_ptr->commit_version is not needed.
    --prepared_entities;
}

void KVAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    AccessEntityPtr old_entity, new_entity;
    Entry* entry_ptr = nullptr;
    Int64 current_version = -1;
    AccessEntityModel *old_entity_model_ptr = nullptr;
    String new_name;
    EntityType type;
    bool name_changed = false;
    bool rollback = true;
    auto & entries_map = entries_by_id[getShard(id)];
    std::unordered_map<String, Entry *> *entries_by_new_name = nullptr, *entries_by_old_name = nullptr;
    {
        std::lock_guard lock{mutex};
        auto it = entries_map.find(id);
        if (it == entries_map.end())
            throwNotFound(id);

        // apply updates on old entity object
        Entry & entry = it->second;
        if (entry.prepare_verison != entry.commit_version)
        {
            // We do not deal with concurrent insert/update conflict.
            throw Exception("Concurrent rbac update: " + entry.name, ErrorCodes::CONCURRENT_RBAC_UPDATE);
        }
        
        entry_ptr = &entry;
        type = entry.type;
        old_entity = entry.entity;
        old_entity_model_ptr = &entry.entity_model;
        new_entity = update_func(old_entity);// NOTE: new entity object now contains sensitive permissions

        new_name = new_entity->getName();
        const String & old_name = old_entity->getName();
        
        entries_by_new_name = &entries_by_name_and_type[static_cast<size_t>(type)][getShard(new_name)];

        // check if newly renamed entity clashes with another name that we have stored
        name_changed = (new_name != old_name);
        if (name_changed)
        {
            if (entries_by_new_name->count(new_name))
                throwNameCollisionCannotRename(type, old_name, new_name);
        }

        // check if new and old entity are of different types
        if (!new_entity->isTypeOf(old_entity->getType()))
            throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

        ++entry_ptr->prepare_verison;
        current_version = entry_ptr->prepare_verison;
        prepared_entities++;
    }

    SCOPE_EXIT({
        if (rollback)
        {
            //Rollback
            std::lock_guard lock{mutex};
            --prepared_entities;
            if (entry_ptr)
                --entry_ptr->prepare_verison;
        }
    });

    if (*new_entity == *old_entity)
        return;

    // write to KV
    AccessEntityModel new_entity_model;
    RPCHelpers::fillUUID(id, *(new_entity_model.mutable_uuid()));
    new_entity_model.set_name(new_name);
    new_entity_model.set_create_sql(convertFromEntityToSql(*new_entity));

    catalog->putAccessEntity(type, new_entity_model, *old_entity_model_ptr);

    rollback = false;
    std::lock_guard lock{mutex};
    entry_ptr->refreshTime();
    entry_ptr->entity = new_entity;
    entry_ptr->commit_time = new_entity_model.commit_time();

    entry_ptr->entity_model = new_entity_model;

    // if renamed, change entry in memory
    if (name_changed)
    {
        entries_by_old_name = &entries_by_name_and_type[static_cast<size_t>(type)][getShard(old_entity->getName())];
        entries_by_old_name->erase(entry_ptr->name);
        entry_ptr->name = new_entity->getName();
        (*entries_by_new_name)[entry_ptr->name] = entry_ptr;
    }

    prepareNotifications(id, *entry_ptr, false, notifications);
    ++entry_ptr->commit_version;
    --prepared_entities;
}

void KVAccessStorage::prepareNotifications(const UUID & id, const Entry & entry, bool remove, Notifications & notifications) const
{
    if (!remove && !entry.entity)
        return;

    const AccessEntityPtr entity = remove ? nullptr : entry.entity;
    for (const auto & handler : entry.handlers_by_id)
        notifications.push_back({handler, id, entity});

    for (const auto & handler : handlers_by_type[static_cast<size_t>(entry.type)])
        notifications.push_back({handler, id, entity});
}


scope_guard KVAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    auto & entries_map = entries_by_id[getShard(id)];
    std::lock_guard lock{mutex};
    auto it = entries_map.find(id);
    if (it == entries_map.end())
        return {};
    const Entry & entry = it->second;
    auto handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler);

    return [this, id, handler_it, &entries_map]
    {
        std::lock_guard lock2{mutex};
        auto it2 = entries_map.find(id);
        if (it2 != entries_map.end())
        {
            const Entry & entry2 = it2->second;
            entry2.handlers_by_id.erase(handler_it);
        }
    };
}

scope_guard KVAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    handlers.push_back(handler);
    auto handler_it = std::prev(handlers.end());

    return [this, type, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto & handlers2 = handlers_by_type[static_cast<size_t>(type)];
        handlers2.erase(handler_it);
    };
}

bool KVAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    auto & entries_map = entries_by_id[getShard(id)];
    std::lock_guard lock{mutex};
    auto it = entries_map.find(id);
    if (it != entries_map.end())
    {
        const Entry & entry = it->second;
        return !entry.handlers_by_id.empty();
    }
    return false;
}

bool KVAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    return !handlers.empty();
}

}
