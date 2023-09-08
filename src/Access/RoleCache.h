#pragma once

#include <Access/EnabledRoles.h>
#include <Poco/AccessExpireCache.h>
#include <boost/container/flat_set.hpp>
#include <map>
#include <mutex>


namespace DB
{
class AccessControlManager;
struct Role;
using RolePtr = std::shared_ptr<const Role>;

class RoleCache
{
public:
    RoleCache(const AccessControlManager & manager_);
    ~RoleCache();

    std::shared_ptr<const EnabledRoles> getEnabledRoles(
        const std::vector<UUID> & current_roles,
        const std::vector<UUID> & current_roles_with_admin_option);

private:
    using SubscriptionsOnRoles = std::vector<std::shared_ptr<scope_guard>>;

    void collectEnabledRoles(scope_guard * notifications);
    void collectEnabledRoles(EnabledRoles & enabled_roles, SubscriptionsOnRoles & subscriptions_on_roles, scope_guard * notifications);
    RolePtr getRole(const UUID & role_id, SubscriptionsOnRoles & subscriptions_on_roles);
    void roleChanged();

    const AccessControlManager & manager;

    // AccessExpireCache is not required in our case since our read function in KVAccessStorage will read directly from memory
    struct EnabledRolesWithSubscriptions
    {
        std::weak_ptr<EnabledRoles> enabled_roles;

        /// We need to keep subscriptions for all enabled roles to be able to recalculate EnabledRolesInfo when some of the roles change.
        SubscriptionsOnRoles subscriptions_on_roles;
    };

    std::map<EnabledRoles::Params, EnabledRolesWithSubscriptions> enabled_roles_by_params;

    mutable std::mutex mutex;
};

}
