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
#include <Protos/RPCHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_ACCESS_ENTITY_DEFINITION;
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
    task = context->getSchedulePool().createTask("updateExpiredEntries", [this]() { updateExpiredEntries(); });
    if (task)
        task->activateAndSchedule();
}

void KVAccessStorage::clear()
{
    entries_by_id.clear();
    for (auto type : collections::range(EntityType::MAX))
        // collections::range(MAX_CONDITION_TYPE) give us a range of [0, MAX_CONDITION_TYPE) 
        // coverity[overrun-local]
        entries_by_name_and_type[static_cast<size_t>(type)].clear();
}

void KVAccessStorage::stopBgJob()
{
    if (task)
    {
        task->deactivate();
    }
}


void KVAccessStorage::updateExpiredEntries()
{
    SCOPE_EXIT({
        if (task)
            task->scheduleAfter(ttl_ms / 2); // check half the time of ttl to reduce how long an entry stays stale
    });
    std::vector<std::pair<EntityType, Entry>> expired_entries;
    {
        std::lock_guard lock{mutex};
        for (auto & [id, entry] : entries_by_id)
        {
            if (entry.isExpired(ttl_ms))
                expired_entries.emplace_back(entry.type, entry);
        }
    }

    // for each expired entity, update to latest version. RBAC TODO: implement multiFind here
    for (auto & [type, entry] : expired_entries)
        onAccessEntityChanged(type, entry.name);
}

UUID KVAccessStorage::addEntry(EntityType type, const AccessEntityModel & entity_model, Notifications & notifications) const
{
    UUID uuid = RPCHelpers::createUUID(entity_model.uuid());
    auto & entry = entries_by_id[uuid];
    entry.type = type;
    entry.name = entity_model.name();
    entry.id = uuid;
    entry.commit_time = entity_model.commit_time();
    entry.entity = convertFromSqlToEntity(entity_model.create_sql());
    entry.entity_model = entity_model;
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    entries_by_name[entry.name] = &entry;
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
    auto entity_model = catalog->tryGetAccessEntity(type, name);
    std::lock_guard lock{mutex};
    if (entity_model)
        addEntry(type, *entity_model, notifications);
    else
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        auto it = entries_by_name.find(name);
        if (it != entries_by_name.end())
        {
            Entry & entry = *(it->second);
            prepareNotifications(entry.id, entry, true, notifications);
            entries_by_name.erase(it);
            entries_by_id.erase(entry.id);
        }
    }
}



std::vector<UUID> KVAccessStorage::findAllImpl(EntityType type) const
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });
    auto entity_models = catalog->getAllAccessEntities(type);
    std::lock_guard lock{mutex};
    std::vector<UUID> res;
    res.reserve(entity_models.size());
    for (const auto & entity_model : entity_models)
        res.emplace_back(addEntry(type, entity_model, notifications));
    return res;
}

bool KVAccessStorage::existsImpl(const UUID & id) const
{
    {
        std::lock_guard lock{mutex};
        if (entries_by_id.count(id))
            return true;
    }
    if (catalog->tryGetAccessEntityName((id)))
        return true;
        
    return false;
}


AccessEntityPtr KVAccessStorage::readImpl(const UUID & id) const
{

    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);

    const auto & entry = it->second;
    return entry.entity;
}


String KVAccessStorage::readNameImpl(const UUID & id) const
{
    {
        std::lock_guard lock{mutex};
        auto it = entries_by_id.find(id);
        if (it != entries_by_id.end())
            return String{it->second.name};
    }

    auto name = catalog->tryGetAccessEntityName(id);
    if (!name)
        throwNotFound(id);
    return *name;
}


bool KVAccessStorage::canInsertImpl(const AccessEntityPtr &) const
{
    return true;
}


UUID KVAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    UUID id = UUIDHelpers::generateV4();
    std::lock_guard lock{mutex};
    insertNoLock(id, new_entity, replace_if_exists, notifications);
    return id;
}


void KVAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, Notifications & notifications)
{
    const String & name = new_entity->getName();
    EntityType type = new_entity->getType();

    AccessEntityModel new_entity_model;
    AccessEntityModel old_entity_model;
    RPCHelpers::fillUUID(id, *(new_entity_model.mutable_uuid()));
    new_entity_model.set_name(name);
    new_entity_model.set_create_sql(convertFromEntityToSql(*new_entity));
    catalog->putAccessEntity(type, new_entity_model, old_entity_model, replace_if_exists);

    /// Add entry.
    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.type = type;
    entry.name = name;
    entry.commit_time = new_entity_model.commit_time();
    entry.entity = new_entity;
    entry.entity_model = new_entity_model;
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    entries_by_name[entry.name] = &entry;
    prepareNotifications(id, entry, false, notifications);
}


void KVAccessStorage::removeImpl(const UUID & id)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    removeNoLock(id, notifications);
}


void KVAccessStorage::removeNoLock(const UUID & id, Notifications & notifications)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);

    Entry & entry = it->second;
    EntityType type = entry.type;
    String name = entry.name;

    catalog->dropAccessEntity(type, id, name);

    /// Do removing.
    prepareNotifications(id, entry, true, notifications);
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    entries_by_name.erase(name);
    entries_by_id.erase(it);
}


void KVAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    std::lock_guard lock{mutex};
    updateNoLock(id, update_func, notifications);
}

void KVAccessStorage::updateNoLock(const UUID & id, const UpdateFunc & update_func, [[maybe_unused]] Notifications & notifications)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        throwNotFound(id);

    // apply updates on old entity object
    Entry & entry = it->second;
    auto old_entity = entry.entity;
    auto new_entity = update_func(old_entity);

    // check if new and old entity are of different types
    if (!new_entity->isTypeOf(old_entity->getType()))
        throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

    if (*new_entity == *old_entity)
        return;

    const String & new_name = new_entity->getName();
    const String & old_name = old_entity->getName();
    const EntityType type = entry.type;
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];

    // check if newly renamed entity clashes with another name that we have stored
    bool name_changed = (new_name != old_name);
    if (name_changed)
    {
        if (entries_by_name.count(new_name) || catalog->tryGetAccessEntity(type, new_name))
            throwNameCollisionCannotRename(type, old_name, new_name);
    }

    // write to KV
    AccessEntityModel new_entity_model;
    RPCHelpers::fillUUID(id, *(new_entity_model.mutable_uuid()));
    new_entity_model.set_name(new_name);
    new_entity_model.set_create_sql(convertFromEntityToSql(*new_entity));

    catalog->putAccessEntity(type, new_entity_model, entry.entity_model);
    entry.entity = new_entity;
    entry.entity_model = new_entity_model;

    // if renamed, change entry in memory
    if (name_changed)
    {
        entries_by_name.erase(entry.name);
        entry.name = new_name;
        entries_by_name[entry.name] = &entry;
    }

    prepareNotifications(id, entry, false, notifications);
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
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
        return {};
    const Entry & entry = it->second;
    auto handler_it = entry.handlers_by_id.insert(entry.handlers_by_id.end(), handler);

    return [this, id, handler_it]
    {
        std::lock_guard lock2{mutex};
        auto it2 = entries_by_id.find(id);
        if (it2 != entries_by_id.end())
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
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it != entries_by_id.end())
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
