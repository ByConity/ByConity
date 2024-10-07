#include <Access/ContextAccess.h>
#include <Access/AccessControlManager.h>
#include <Access/EnabledRoles.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledSettings.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/KVAccessStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <assert.h>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int FUNCTION_NOT_ALLOWED;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_USER;
}


namespace
{
    /* must be synced with the Level definition in AccessRights.cpp */
    enum Level
    {
        GLOBAL_LEVEL,
        DATABASE_LEVEL,
        TABLE_LEVEL,
        COLUMN_LEVEL,
    };

    static const std::unordered_set<std::string_view> always_accessible_tables {
        /// Constant tables
        "one",

        /// "numbers", "numbers_mt", "zeros", "zeros_mt" were excluded because they can generate lots of values and
        /// that can decrease performance in some cases.
        /// We limit the number of values to 100K for multiple tenants case
        "numbers",
        "numbers_mt",
        "zeros",
        "zeros_mt",

        "contributors",
        "licenses",
        "time_zones",
        "collations",

        "formats",
        "privileges",
        "data_type_families",
        "table_engines",
        "table_functions",
        "aggregate_function_combinators",

        "functions", /// Can contain user-defined functions
        "cnch_user_defined_functions",

        /// The following tables hide some rows if the current user doesn't have corresponding SHOW privileges.
        "databases",
        "tables",
        "columns",
        "mutations",
        "users",
        "dictionaries",

        /// Specific to the current session
        "settings",
        "current_roles",
        "enabled_roles",
        "quota_usage",
        "processes",

        /// parts related
        "parts",
        "detached_parts",
        "projection_parts",
        "parts_columns",
        "projection_parts_columns",

        /// The following tables hide some rows if the current user doesn't have corresponding SHOW privileges.
        /// For IDE tools to get schema info
        "cnch_databases",
        "cnch_tables",
        "cnch_columns",
        "cnch_parts",
        "cnch_parts_info",
        "data_skipping_indices",
        "external_catalogs",
        "external_tables",
        "external_databases"
    };

    AccessRights mixAccessRightsFromUserAndRoles(const User & user, const EnabledRolesInfo & roles_info)
    {
        AccessRights res = user.access;
        res.makeUnion(roles_info.access);
        return res;
    }

    SensitiveAccessRights mixSensitiveAccessRightsFromUserAndRoles(const User & user, const EnabledRolesInfo & roles_info)
    {
        SensitiveAccessRights res = user.sensitive_access;
        res.makeUnion(roles_info.sensitive_access);
        return res;
    }

    AccessRights addImplicitAccessRights(const AccessRights & access, const AccessControlManager & manager, bool has_tenant_id_in_username)
    {
        AccessFlags max_flags;

        auto modifier = [&](const AccessFlags & flags,
                            const AccessFlags & min_flags_with_children,
                            const AccessFlags & max_flags_with_children,
                            std::string_view database,
                            std::string_view table,
                            std::string_view column,
                            bool /* grant_option */) -> AccessFlags
        {
            size_t level = !database.empty() + !table.empty() + !column.empty();
            AccessFlags res = flags;

            /// CREATE_TABLE => CREATE_VIEW, DROP_TABLE => DROP_VIEW, ALTER_TABLE => ALTER_VIEW
            static const AccessFlags create_table = AccessType::CREATE_TABLE;
            static const AccessFlags create_view = AccessType::CREATE_VIEW;
            static const AccessFlags drop_table = AccessType::DROP_TABLE;
            static const AccessFlags drop_view = AccessType::DROP_VIEW;
            static const AccessFlags alter_table = AccessType::ALTER_TABLE;
            static const AccessFlags alter_view = AccessType::ALTER_VIEW;

            if (res & create_table)
                res |= create_view;

            if (res & drop_table)
                res |= drop_view;

            if (res & alter_table)
                res |= alter_view;

            /// CREATE TABLE (on any database/table) => CREATE_TEMPORARY_TABLE (global)
            static const AccessFlags create_temporary_table = AccessType::CREATE_TEMPORARY_TABLE;
            if ((level == 0) && (max_flags_with_children & create_table))
                res |= create_temporary_table;

            /// ALTER_TTL => ALTER_MATERIALIZE_TTL
            static const AccessFlags alter_ttl = AccessType::ALTER_TTL;
            static const AccessFlags alter_materialize_ttl = AccessType::ALTER_MATERIALIZE_TTL;
            if (res & alter_ttl)
                res |= alter_materialize_ttl;

            /// RELOAD_DICTIONARY (global) => RELOAD_EMBEDDED_DICTIONARIES (global)
            static const AccessFlags reload_dictionary = AccessType::SYSTEM_RELOAD_DICTIONARY;
            static const AccessFlags reload_embedded_dictionaries = AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES;
            if ((level == 0) && (min_flags_with_children & reload_dictionary))
                res |= reload_embedded_dictionaries;

            /// any column flag => SHOW_COLUMNS => SHOW_TABLES => SHOW_DATABASES
            ///                  any table flag => SHOW_TABLES => SHOW_DATABASES
            ///       any dictionary flag => SHOW_DICTIONARIES => SHOW_DATABASES
            ///                              any database flag => SHOW_DATABASES
            static const AccessFlags show_columns = AccessType::SHOW_COLUMNS;
            static const AccessFlags show_tables = AccessType::SHOW_TABLES;
            static const AccessFlags show_dictionaries = AccessType::SHOW_DICTIONARIES;
            static const AccessFlags show_tables_or_dictionaries = show_tables | show_dictionaries;
            static const AccessFlags show_databases = AccessType::SHOW_DATABASES;

            if (res & AccessFlags::allColumnFlags())
                res |= show_columns;

            if ((res & AccessFlags::allTableFlags())
                || (level <= 2 && (res & show_columns))
                || (level == 2 && (max_flags_with_children & show_columns)))
            {
                res |= show_tables;
            }

            if (res & AccessFlags::allDictionaryFlags())
                res |= show_dictionaries;

            if ((res & AccessFlags::allDatabaseFlags())
                || (level <= 1 && (res & show_tables_or_dictionaries))
                || (level == 1 && (max_flags_with_children & show_tables_or_dictionaries)))
            {
                res |= show_databases;
            }

            max_flags |= res;

            return res;
        };

        AccessRights res = access;
        res.modifyFlags(modifier);

        /// If "select_from_system_db_requires_grant" is enabled we provide implicit grants only for a few tables in the system database.
        if (manager.doesSelectFromSystemDatabaseRequireGrant() || has_tenant_id_in_username)
        {
            for (const auto & table_name : always_accessible_tables)
                res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, table_name);

            if (max_flags.contains(AccessType::SHOW_USERS))
                res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "users");

            if (max_flags.contains(AccessType::SHOW_ROLES))
                res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "roles");

            if (max_flags.contains(AccessType::SHOW_ROW_POLICIES))
                res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "row_policies");

            if (max_flags.contains(AccessType::SHOW_SETTINGS_PROFILES))
                res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "settings_profiles");

            if (max_flags.contains(AccessType::SHOW_QUOTAS))
                res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "quotas");
        }
        else
        {
            res.grant(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE);
        }

        /// If "select_from_information_schema_requires_grant" is enabled we don't provide implicit grants for the information_schema database.
        if (!manager.doesSelectFromInformationSchemaRequireGrant())
        {
            res.grant(AccessType::SELECT, DatabaseCatalog::INFORMATION_SCHEMA);
            res.grant(AccessType::SELECT, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE);
        }

        if (!manager.doesSelectFromMySQLRequireGrant())
        {
            res.grant(AccessType::SELECT, DatabaseCatalog::MYSQL);
            res.grant(AccessType::SELECT, DatabaseCatalog::MYSQL_UPPERCASE);
        }
        // information_schema is always visible
        res.grant(AccessType::SHOW_DATABASES, DatabaseCatalog::INFORMATION_SCHEMA);
        res.grant(AccessType::SHOW_DATABASES, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE);

        res.grant(AccessType::SHOW_DATABASES, DatabaseCatalog::MYSQL);
        res.grant(AccessType::SHOW_DATABASES, DatabaseCatalog::MYSQL_UPPERCASE);

        return res;
    }


    std::array<UUID, 1> to_array(const UUID & id)
    {
        std::array<UUID, 1> ids;
        ids[0] = id;
        return ids;
    }

    /// Helper for using in templates.
    std::string_view getDatabase() { return {}; }

    template <typename... OtherArgs>
    std::string_view getDatabase(const std::string_view & arg1, const OtherArgs &...) { return arg1; }
}

bool ContextAccess::isAlwaysAccessibleTableInSystem(const std::string_view & table) const
{
    if (!params.has_tenant_id_in_username)
        return true;

    return always_accessible_tables.contains(table);
}

ContextAccess::ContextAccess(const AccessControlManager & manager_, const Params & params_)
    : manager(&manager_)
    , params(params_)
{
}

void ContextAccess::initialize(bool load_roles)
{
    if (!params.user_id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No user in current context, it's a bug");

    std::lock_guard lock{mutex};

    subscription_for_user_change = manager->subscribeForChanges(
        *params.user_id,
        [weak_ptr = weak_from_this()](const UUID &, const AccessEntityPtr & entity)
        {
            auto ptr = weak_ptr.lock();
            if (!ptr)
                return;
            UserPtr changed_user = entity ? typeid_cast<UserPtr>(entity) : nullptr;
            std::lock_guard lock2{ptr->mutex};
            ptr->setUser(changed_user);
        });


    auto user_ = manager->read<User>(*params.user_id);
    if (load_roles && params.use_default_roles)
        loadRoles(user_);
    setUser(user_);
}

void ContextAccess::loadRoles(const UserPtr & user_) const
{
    std::unordered_set<UUID> ids;
    ids.reserve(user_->default_roles.ids.size() + user_->default_roles.except_ids.size() + params.current_roles.size());
    boost::range::copy(user_->default_roles.ids, std::inserter(ids, ids.end()));
    boost::range::copy(user_->default_roles.except_ids, std::inserter(ids, ids.end()));
    boost::range::copy(params.current_roles, std::inserter(ids, ids.end()));

    if (ids.empty())
        return;

    for (const auto & storage : *const_cast<AccessControlManager *>(manager)->getStoragesPtr())
    {
        auto kv = typeid_cast<std::shared_ptr<KVAccessStorage>>(storage);
        if (!kv)
            continue;

        kv->loadEntities(IAccessEntity::Type::ROLE, ids);
        break;
    }
}

void ContextAccess::setUser(const UserPtr & user_) const
{
    user = user_;
    if (!user)
    {
        /// User has been dropped.
        subscription_for_user_change = {};
        subscription_for_roles_changes = {};
        access = nullptr;
        access_with_implicit = nullptr;
        sensitive_access = nullptr;
        enabled_roles = nullptr;
        roles_info = nullptr;
        enabled_row_policies = nullptr;
        enabled_quota = nullptr;
        enabled_settings = nullptr;
        return;
    }

    user_name = user->getName();
    trace_log = getLogger("ContextAccess (" + user_name + ")");

    std::vector<UUID> current_roles, current_roles_with_admin_option;

    if (params.use_default_roles)
    {
        current_roles = user->granted_roles.findGranted(user->default_roles);
        current_roles_with_admin_option = user->granted_roles.findGrantedWithAdminOption(user->default_roles);
    }
    else
    {
        current_roles = user->granted_roles.findGranted(params.current_roles);
        current_roles_with_admin_option = user->granted_roles.findGrantedWithAdminOption(params.current_roles);
    }

    subscription_for_roles_changes.reset();
    enabled_roles = manager->getEnabledRoles(current_roles, current_roles_with_admin_option);
    subscription_for_roles_changes = enabled_roles->subscribeForChanges([this](const std::shared_ptr<const EnabledRolesInfo> & roles_info_)
    {
        std::lock_guard lock{mutex};
        setRolesInfo(roles_info_);
    });

    setRolesInfo(enabled_roles->getRolesInfo());
}


void ContextAccess::setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & roles_info_) const
{
    assert(roles_info_);
    roles_info = roles_info_;
    enabled_row_policies = manager->getEnabledRowPolicies(
        *params.user_id, roles_info->enabled_roles);
    enabled_quota = manager->getEnabledQuota(
        *params.user_id, user_name, roles_info->enabled_roles, params.address, params.forwarded_address, params.quota_key);
    enabled_settings = manager->getEnabledSettings(
        *params.user_id, user->settings, roles_info->enabled_roles, roles_info->settings_from_enabled_roles);
    calculateAccessRights();
}


void ContextAccess::calculateAccessRights() const
{
    access = std::make_shared<AccessRights>(mixAccessRightsFromUserAndRoles(*user, *roles_info));
    access_with_implicit = std::make_shared<AccessRights>(addImplicitAccessRights(*access, *manager, params.has_tenant_id_in_username));
    sensitive_access = std::make_shared<SensitiveAccessRights>(mixSensitiveAccessRightsFromUserAndRoles(*user, *roles_info));

    if (trace_log)
    {
        if (roles_info && !roles_info->getCurrentRolesNames().empty())
        {
            LOG_TRACE(trace_log, "Current_roles: {}, enabled_roles: {}",
                boost::algorithm::join(roles_info->getCurrentRolesNames(), ", "),
                boost::algorithm::join(roles_info->getEnabledRolesNames(), ", "));
        }
        LOG_TRACE(trace_log, "Settings: readonly={}, allow_ddl={}, allow_introspection_functions={}", params.readonly, params.allow_ddl, params.allow_introspection);
        LOG_TRACE(trace_log, "List of all grants: {}", access->toString());
        if (params.enable_sensitive_permission)
            LOG_TRACE(trace_log, "List of all sensitive grants: {}", sensitive_access->toString());
        LOG_TRACE(trace_log, "List of all grants including implicit: {}", access_with_implicit->toString());
    }
}


UserPtr ContextAccess::getUser() const
{
    std::lock_guard lock{mutex};
    return user;
}

String ContextAccess::getUserName() const
{
    std::lock_guard lock{mutex};
    return user_name;
}

std::shared_ptr<const EnabledRolesInfo> ContextAccess::getRolesInfo() const
{
    std::lock_guard lock{mutex};
    if (roles_info)
        return roles_info;
    static const auto no_roles = std::make_shared<EnabledRolesInfo>();
    return no_roles;
}

std::shared_ptr<const EnabledRowPolicies> ContextAccess::getEnabledRowPolicies() const
{
    std::lock_guard lock{mutex};
    if (enabled_row_policies)
        return enabled_row_policies;
    static const auto no_row_policies = std::make_shared<EnabledRowPolicies>();
    return no_row_policies;
}

ASTPtr ContextAccess::getRowPolicyCondition(const String & database, const String & table_name, RowPolicy::ConditionType index, const ASTPtr & extra_condition) const
{
    std::lock_guard lock{mutex};
    if (enabled_row_policies)
        return enabled_row_policies->getCondition(database, table_name, index, extra_condition);
    return nullptr;
}

std::shared_ptr<const EnabledQuota> ContextAccess::getQuota() const
{
    std::lock_guard lock{mutex};
    if (enabled_quota)
        return enabled_quota;
    static const auto unlimited_quota = EnabledQuota::getUnlimitedQuota();
    return unlimited_quota;
}


std::optional<QuotaUsage> ContextAccess::getQuotaUsage() const
{
    std::lock_guard lock{mutex};
    if (enabled_quota)
        return enabled_quota->getUsage();
    return {};
}


std::shared_ptr<const ContextAccess> ContextAccess::getFullAccess()
{
    static const std::shared_ptr<const ContextAccess> res = []
    {
        auto full_access = std::shared_ptr<ContextAccess>(new ContextAccess);
        full_access->is_full_access = true;
        full_access->access = std::make_shared<AccessRights>(AccessRights::getFullAccess());
        full_access->access_with_implicit = full_access->access;
        return full_access;
    }();
    return res;
}


SettingsChanges ContextAccess::getDefaultSettings() const
{
    std::lock_guard lock{mutex};
    if (enabled_settings)
    {
        if (auto info = enabled_settings->getInfo())
            return info->settings;
    }
    return {};
}

std::shared_ptr<const SettingsProfilesInfo> ContextAccess::getDefaultProfileInfo() const
{
    std::lock_guard lock{mutex};
    if (enabled_settings)
        return enabled_settings->getInfo();
    static const auto everything_by_default = std::make_shared<SettingsProfilesInfo>(*manager);
    return everything_by_default;
}


std::shared_ptr<const AccessRights> ContextAccess::getAccessRights() const
{
    std::lock_guard lock{mutex};
    if (access)
        return access;
    static const auto nothing_granted = std::make_shared<AccessRights>();
    return nothing_granted;
}


std::shared_ptr<const AccessRights> ContextAccess::getAccessRightsWithImplicit() const
{
    std::lock_guard lock{mutex};
    if (access_with_implicit)
        return access_with_implicit;
    static const auto nothing_granted = std::make_shared<AccessRights>();
    return nothing_granted;
}

std::shared_ptr<const SensitiveAccessRights> ContextAccess::getSensitiveAccessRights() const
{
    std::lock_guard lock{mutex};
    if (sensitive_access)
        return sensitive_access;
    static const auto nothing_granted = std::make_shared<SensitiveAccessRights>();
    return nothing_granted;
}

int ContextAccess::isSensitiveImpl(std::unordered_set<std::string_view> & cols, const std::string_view & database, const std::string_view & table = {}, const std::vector<std::string_view> & columns = {}) const
{
    auto sensitive_resource = manager->sensitive_resource_getter(formatTenantDatabaseName(std::string(database)));
    if (!sensitive_resource)
        return false;

    if (!columns.empty())
    {
        // Find table belonging to columns and check that all input columns are sensitive
        for (const auto & sensitive_table : sensitive_resource->tables())
        {
            if (sensitive_table.table() != table)
                continue;

            std::unordered_set<std::string_view> sensitive_columns(sensitive_table.sensitive_columns().cbegin(), sensitive_table.sensitive_columns().cend());

            for (const auto & col : columns)
            {
                if (sensitive_columns.contains(col))
                    cols.insert(col);
            }

            if (!cols.empty())
                    return COLUMN_LEVEL;
        }
    }

    if (!table.empty())
    {
        for (auto & sensitive_table : sensitive_resource->tables())
        {
            if (sensitive_table.table() != table)
                    continue;

            if (sensitive_table.is_sensitive())
                return TABLE_LEVEL;
        }
    }

    if (!database.empty())
    {
        if (sensitive_resource->is_sensitive())
            return DATABASE_LEVEL;
    }

    return GLOBAL_LEVEL;
}

template <bool throw_if_denied, typename... Args>
static bool checkTenantsAccess(const Args &... args)
{
    if constexpr (sizeof...(args) >= 1)
    {
        const auto & tuple = std::make_tuple(args...);
        const auto & database = std::get<0>(tuple);

        if (!current_thread)
            return true;

        /* always disable access to default database for tenants */
        if (database == "default" || database == "cnch_system")
        {
            const auto ctx = current_thread->getQueryContext();

            if (!ctx)
                return true;

            if (ctx->getAccess()->getParams().has_tenant_id_in_username)
            {
                if constexpr (throw_if_denied)
                    throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} not found.", database);
                return false;
            }
        }

        if constexpr (sizeof...(args) >= 2)
        {
            /* always disable access to system tables for tenants */
            if (database != "system")
                return true;

            const auto ctx = current_thread->getQueryContext();

            if (!ctx)
                return true;

            const auto & table = std::get<1>(tuple);
            return ctx->getAccess()->isAlwaysAccessibleTableInSystem(table);
        }
    }

    return true;
}

template <bool throw_if_denied, bool grant_option, typename... Args>
bool ContextAccess::checkAccessImplHelper(const AccessFlags & flags, const Args &... args) const
{
    auto access_granted = [&]
    {
        if (trace_log)
            LOG_TRACE(trace_log, "Access granted: {}{}", (AccessRightsElement{flags, args...}.toStringWithoutOptions()),
                      (grant_option ? " WITH GRANT OPTION" : ""));
        return true;
    };

    auto access_denied = [&](const String & error_msg, int error_code [[maybe_unused]])
    {
        if (trace_log)
            LOG_TRACE(trace_log, "Access denied: {}{}", (AccessRightsElement{flags, args...}.toStringWithoutOptions()),
                      (grant_option ? " WITH GRANT OPTION" : ""));
        if constexpr (throw_if_denied)
            throw Exception(getUserName() + ": " + error_msg, error_code);
        return false;
    };

    if (!flags || is_full_access)
        return access_granted();

    if (!getUser())
        return access_denied("User has been dropped", ErrorCodes::UNKNOWN_USER);

    /// Access to temporary tables is controlled in an unusual way, not like normal tables.
    /// Creating of temporary tables is controlled by AccessType::CREATE_TEMPORARY_TABLES grant,
    /// and other grants are considered as always given.
    /// The DatabaseCatalog class won't resolve StorageID for temporary tables
    /// which shouldn't be accessed.
    if (getDatabase(args...) == DatabaseCatalog::TEMPORARY_DATABASE)
        return access_granted();

    auto acs = getAccessRightsWithImplicit();
    bool granted;
    bool check_sensitive_permissions = false;

    if constexpr (grant_option)
        granted = acs->hasGrantOption(flags, args...);
    else
        granted = acs->isGranted(flags, args...);

    if (granted)
        granted = checkTenantsAccess<throw_if_denied>(args...);

    while (granted)
    {
        auto tenant_id = getCurrentTenantId();

        // Only apply sensitive permission checks on tenanted users only
        if (tenant_id.empty())
            break;

        // Only enable sensitive permission check for selected tenants
        if (!params.enable_sensitive_permission)
            break;

        if (roles_info->is_admin)
            break;

        std::unordered_set<std::string_view> sensitive_columns;
        int sensitive_level = isSensitive(sensitive_columns, args...);

        if (sensitive_level != GLOBAL_LEVEL)
        {
            check_sensitive_permissions = true;
            //std::vector<std::string_view> cols{sensitive_columns.begin(), sensitive_columns.end()};
            granted = getSensitiveAccessRights()->isGranted(sensitive_level, sensitive_columns, flags, args...);
        }
        break;
    }

    if (!granted)
    {
        if (grant_option && acs->isGranted(flags, args...))
        {
            return access_denied(
                "Not enough privileges. "
                "The required privileges have been granted, but without grant option. "
                "To execute this query it's necessary to have grant "
                    + AccessRightsElement{flags, args...}.toStringWithoutOptions() + " WITH GRANT OPTION",
                ErrorCodes::ACCESS_DENIED);
        }

        return access_denied(
            "Not enough privileges. To execute this query it's necessary to have grant"
                + (check_sensitive_permissions && !grant_option ? std::string(" SENSITIVE ") : std::string(" "))
                + AccessRightsElement{flags, args...}.toStringWithoutOptions() + (grant_option ? " WITH GRANT OPTION" : ""),
            ErrorCodes::ACCESS_DENIED);
    }

    struct PrecalculatedFlags
    {
        const AccessFlags table_ddl = AccessType::CREATE_DATABASE | AccessType::CREATE_TABLE | AccessType::CREATE_VIEW
            | AccessType::ALTER_TABLE | AccessType::ALTER_VIEW | AccessType::DROP_DATABASE | AccessType::DROP_TABLE | AccessType::DROP_VIEW
            | AccessType::TRUNCATE;

        const AccessFlags dictionary_ddl = AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY;
        const AccessFlags function_ddl = AccessType::CREATE_FUNCTION | AccessType::DROP_FUNCTION;
        const AccessFlags binding_ddl = AccessType::CREATE_BINDING | AccessType::DROP_BINDING;
        const AccessFlags prepared_statement_ddl = AccessType::CREATE_PREPARED_STATEMENT | AccessType::DROP_PREPARED_STATEMENT;
        const AccessFlags table_and_dictionary_ddl = table_ddl | dictionary_ddl;
        const AccessFlags table_and_dictionary_and_function_ddl_and_binding = table_ddl | dictionary_ddl | function_ddl | binding_ddl | prepared_statement_ddl;
        const AccessFlags write_table_access = AccessType::INSERT | AccessType::OPTIMIZE;
        const AccessFlags write_dcl_access = AccessType::ACCESS_MANAGEMENT - AccessType::SHOW_ACCESS;

        const AccessFlags not_readonly_flags = write_table_access | table_and_dictionary_and_function_ddl_and_binding | write_dcl_access | AccessType::SYSTEM | AccessType::KILL_QUERY;
        const AccessFlags not_readonly_1_flags = AccessType::CREATE_TEMPORARY_TABLE;

        const AccessFlags ddl_flags = table_ddl | dictionary_ddl | function_ddl | binding_ddl | prepared_statement_ddl;
        const AccessFlags introspection_flags = AccessType::INTROSPECTION;
    };
    static const PrecalculatedFlags precalc;

    if (params.readonly)
    {
        if constexpr (grant_option)
            return access_denied("Cannot change grants in readonly mode.", ErrorCodes::READONLY);
        if ((flags & precalc.not_readonly_flags) ||
            ((params.readonly == 1) && (flags & precalc.not_readonly_1_flags)))
        {
            if (params.interface == ClientInfo::Interface::HTTP && params.http_method == ClientInfo::HTTPMethod::GET)
            {
                return access_denied(
                    "Cannot execute query in readonly mode. "
                    "For queries over HTTP, method GET implies readonly. You should use method POST for modifying queries",
                    ErrorCodes::READONLY);
            }
            else
                return access_denied("Cannot execute query in readonly mode", ErrorCodes::READONLY);
        }
    }

    if (!params.allow_ddl && !grant_option)
    {
        if (flags & precalc.ddl_flags)
            return access_denied("Cannot execute query. DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
    }

    if (!params.allow_introspection && !grant_option)
    {
        if (flags & precalc.introspection_flags)
            return access_denied("Introspection functions are disabled, because setting 'allow_introspection_functions' is set to 0", ErrorCodes::FUNCTION_NOT_ALLOWED);
    }

    return access_granted();
}

template <bool throw_if_denied, bool grant_option>
bool ContextAccess::checkAccessImpl(const AccessFlags & flags) const
{
    return checkAccessImplHelper<throw_if_denied, grant_option>(flags);
}

template <bool throw_if_denied, bool grant_option, typename... Args>
bool ContextAccess::checkAccessImpl(const AccessFlags & flags, const std::string_view & database, const Args &... args) const
{
    return checkAccessImplHelper<throw_if_denied, grant_option>(flags, database.empty() ? params.current_database : database, args...);
}

template <bool throw_if_denied, bool grant_option>
bool ContextAccess::checkAccessImplHelper(const AccessRightsElement & element) const
{
    assert(!element.grant_option || grant_option);
    if (element.any_database)
        return checkAccessImpl<throw_if_denied, grant_option>(element.access_flags);
    else if (element.any_table)
        return checkAccessImpl<throw_if_denied, grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        return checkAccessImpl<throw_if_denied, grant_option>(element.access_flags, element.database, element.table);
    else
        return checkAccessImpl<throw_if_denied, grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool throw_if_denied, bool grant_option>
bool ContextAccess::checkAccessImpl(const AccessRightsElement & element) const
{
    if constexpr (grant_option)
    {
        return checkAccessImplHelper<throw_if_denied, true>(element);
    }
    else
    {
        if (element.grant_option)
            return checkAccessImplHelper<throw_if_denied, true>(element);
        else
            return checkAccessImplHelper<throw_if_denied, false>(element);
    }
}

template <bool throw_if_denied, bool grant_option>
bool ContextAccess::checkAccessImpl(const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        if (!checkAccessImpl<throw_if_denied, grant_option>(element))
            return false;
    return true;
}

int ContextAccess::isSensitive(std::unordered_set<std::string_view> & /*cols*/) { return GLOBAL_LEVEL; }
int ContextAccess::isSensitive(std::unordered_set<std::string_view> & cols, const std::string_view & database) const { return isSensitiveImpl(cols, database); }
int ContextAccess::isSensitive(std::unordered_set<std::string_view> & cols, const std::string_view & database, const std::string_view & table) const { return isSensitiveImpl(cols, database, table); }
int ContextAccess::isSensitive(std::unordered_set<std::string_view> & cols, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isSensitiveImpl(cols, database, table, {column}); }
int ContextAccess::isSensitive(std::unordered_set<std::string_view> & cols, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isSensitiveImpl(cols, database, table, columns); }
int ContextAccess::isSensitive(std::unordered_set<std::string_view> & cols, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isSensitiveImpl(cols, database, table, {columns.begin(), columns.end()}); }

bool ContextAccess::isGranted(const AccessFlags & flags) const { return checkAccessImpl<false, false>(flags); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database) const { return checkAccessImpl<false, false>(flags, database); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return checkAccessImpl<false, false>(flags, database, table); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkAccessImpl<false, false>(flags, database, table, column); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkAccessImpl<false, false>(flags, database, table, columns); }
bool ContextAccess::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkAccessImpl<false, false>(flags, database, table, columns); }
bool ContextAccess::isGranted(const AccessRightsElement & element) const { return checkAccessImpl<false, false>(element); }
bool ContextAccess::isGranted(const AccessRightsElements & elements) const { return checkAccessImpl<false, false>(elements); }

bool ContextAccess::hasGrantOption(const AccessFlags & flags) const { return checkAccessImpl<false, true>(flags); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database) const { return checkAccessImpl<false, true>(flags, database); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return checkAccessImpl<false, true>(flags, database, table); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkAccessImpl<false, true>(flags, database, table, column); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkAccessImpl<false, true>(flags, database, table, columns); }
bool ContextAccess::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkAccessImpl<false, true>(flags, database, table, columns); }
bool ContextAccess::hasGrantOption(const AccessRightsElement & element) const { return checkAccessImpl<false, true>(element); }
bool ContextAccess::hasGrantOption(const AccessRightsElements & elements) const { return checkAccessImpl<false, true>(elements); }

void ContextAccess::checkAccess(const AccessFlags & flags) const { checkAccessImpl<true, false>(flags); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database) const { checkAccessImpl<true, false>(flags, database); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { checkAccessImpl<true, false>(flags, database, table); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkAccessImpl<true, false>(flags, database, table, column); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkAccessImpl<true, false>(flags, database, table, columns); }
void ContextAccess::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkAccessImpl<true, false>(flags, database, table, columns); }
void ContextAccess::checkAccess(const AccessRightsElement & element) const { checkAccessImpl<true, false>(element); }
void ContextAccess::checkAccess(const AccessRightsElements & elements) const { checkAccessImpl<true, false>(elements); }

void ContextAccess::checkGrantOption(const AccessFlags & flags) const { checkAccessImpl<true, true>(flags); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database) const { checkAccessImpl<true, true>(flags, database); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { checkAccessImpl<true, true>(flags, database, table); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { checkAccessImpl<true, true>(flags, database, table, column); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { checkAccessImpl<true, true>(flags, database, table, columns); }
void ContextAccess::checkGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { checkAccessImpl<true, true>(flags, database, table, columns); }
void ContextAccess::checkGrantOption(const AccessRightsElement & element) const { checkAccessImpl<true, true>(element); }
void ContextAccess::checkGrantOption(const AccessRightsElements & elements) const { checkAccessImpl<true, true>(elements); }


template <bool throw_if_denied, typename Container, typename GetNameFunction>
bool ContextAccess::checkAdminOptionImplHelper(const Container & role_ids, const GetNameFunction & get_name_function) const
{
    if (!std::size(role_ids) || is_full_access)
        return true;

    auto show_error = [this](const String & msg, int error_code [[maybe_unused]])
    {
        UNUSED(this);
        if constexpr (throw_if_denied)
            throw Exception(getUserName() + ": " + msg, error_code);
    };

    if (!getUser())
    {
        show_error("User has been dropped", ErrorCodes::UNKNOWN_USER);
        return false;
    }

    if (isGranted(AccessType::ROLE_ADMIN))
        return true;

    auto info = getRolesInfo();
    size_t i = 0;
    for (auto it = std::begin(role_ids); it != std::end(role_ids); ++it, ++i)
    {
        const UUID & role_id = *it;
        if (info->enabled_roles_with_admin_option.count(role_id))
            continue;

        if (throw_if_denied)
        {
            auto role_name = get_name_function(role_id, i);
            if (!role_name)
                role_name = "ID {" + toString(role_id) + "}";

            if (info->enabled_roles.count(role_id))
                show_error("Not enough privileges. "
                           "Role " + backQuote(*role_name) + " is granted, but without ADMIN option. "
                           "To execute this query it's necessary to have the role " + backQuoteIfNeed(*role_name) + " granted with ADMIN option.",
                           ErrorCodes::ACCESS_DENIED);
            else
                show_error("Not enough privileges. "
                           "To execute this query it's necessary to have the role " + backQuoteIfNeed(*role_name) + " granted with ADMIN option.",
                           ErrorCodes::ACCESS_DENIED);
        }

        return false;
    }

    return true;
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const UUID & role_id) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(to_array(role_id), [this](const UUID & id, size_t) { return manager->tryReadName(id); });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const UUID & role_id, const String & role_name) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(to_array(role_id), [&role_name](const UUID &, size_t) { return std::optional<String>{role_name}; });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(to_array(role_id), [&names_of_roles](const UUID & id, size_t) { auto it = names_of_roles.find(id); return (it != names_of_roles.end()) ? it->second : std::optional<String>{}; });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const std::vector<UUID> & role_ids) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(role_ids, [this](const UUID & id, size_t) { return manager->tryReadName(id); });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(role_ids, [&names_of_roles](const UUID &, size_t i) { return std::optional<String>{names_of_roles[i]}; });
}

template <bool throw_if_denied>
bool ContextAccess::checkAdminOptionImpl(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const
{
    return checkAdminOptionImplHelper<throw_if_denied>(role_ids, [&names_of_roles](const UUID & id, size_t) { auto it = names_of_roles.find(id); return (it != names_of_roles.end()) ? it->second : std::optional<String>{}; });
}

bool ContextAccess::hasAdminOption(const UUID & role_id) const { return checkAdminOptionImpl<false>(role_id); }
bool ContextAccess::hasAdminOption(const UUID & role_id, const String & role_name) const { return checkAdminOptionImpl<false>(role_id, role_name); }
bool ContextAccess::hasAdminOption(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const { return checkAdminOptionImpl<false>(role_id, names_of_roles); }
bool ContextAccess::hasAdminOption(const std::vector<UUID> & role_ids) const { return checkAdminOptionImpl<false>(role_ids); }
bool ContextAccess::hasAdminOption(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const { return checkAdminOptionImpl<false>(role_ids, names_of_roles); }
bool ContextAccess::hasAdminOption(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const { return checkAdminOptionImpl<false>(role_ids, names_of_roles); }

void ContextAccess::checkAdminOption(const UUID & role_id) const { checkAdminOptionImpl<true>(role_id); }
void ContextAccess::checkAdminOption(const UUID & role_id, const String & role_name) const { checkAdminOptionImpl<true>(role_id, role_name); }
void ContextAccess::checkAdminOption(const UUID & role_id, const std::unordered_map<UUID, String> & names_of_roles) const { checkAdminOptionImpl<true>(role_id, names_of_roles); }
void ContextAccess::checkAdminOption(const std::vector<UUID> & role_ids) const { checkAdminOptionImpl<true>(role_ids); }
void ContextAccess::checkAdminOption(const std::vector<UUID> & role_ids, const Strings & names_of_roles) const { checkAdminOptionImpl<true>(role_ids, names_of_roles); }
void ContextAccess::checkAdminOption(const std::vector<UUID> & role_ids, const std::unordered_map<UUID, String> & names_of_roles) const { checkAdminOptionImpl<true>(role_ids, names_of_roles); }

bool ContextAccessParams::dependsOnSettingName(std::string_view setting_name)
{
    return (setting_name == "readonly") || (setting_name == "allow_ddl") || (setting_name == "allow_introspection_functions");
}

}
