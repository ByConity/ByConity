#pragma once

#include <common/types.h>
#include <Access/AccessRightsElement.h>
#include <memory>
#include <vector>
#include <unordered_set>


namespace DB
{
/// Represents a set of access types granted on databases, tables, columns, etc.
/// For example, "GRANT SELECT, UPDATE ON db.*, GRANT INSERT ON db2.mytbl2" are access rights.
template <bool IsSensitive>
class AccessRightsBase
{
public:
    AccessRightsBase();
    explicit AccessRightsBase(const AccessFlags & access);
    explicit AccessRightsBase(const AccessRightsElement & element);
    explicit AccessRightsBase(const AccessRightsElements & elements);

    ~AccessRightsBase();
    AccessRightsBase(const AccessRightsBase & src);
    AccessRightsBase & operator =(const AccessRightsBase & src);
    AccessRightsBase(AccessRightsBase && src) noexcept;
    AccessRightsBase & operator =(AccessRightsBase && src) noexcept;

    bool isEmpty() const;

    /// Revokes everything. It's the same as revoke(AccessType::ALL).
    void clear();

    /// Returns the information about all the access granted as a string.
    String toString() const;

    /// Returns the information about all the access granted.
    AccessRightsElements getElements() const;

    /// Grants access on a specified database/table/column.
    /// Does nothing if the specified access has been already granted.
    void grant(const AccessFlags & flags);
    void grant(const AccessFlags & flags, const std::string_view & database);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void grant(const AccessRightsElement & element);
    void grant(const AccessRightsElements & elements);

    void grantWithGrantOption(const AccessFlags & flags);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void grantWithGrantOption(const AccessRightsElement & element);
    void grantWithGrantOption(const AccessRightsElements & elements);

    /// Revokes a specified access granted earlier on a specified database/table/column.
    /// For example, revoke(AccessType::ALL) revokes all grants at all, just like clear();
    void revoke(const AccessFlags & flags);
    void revoke(const AccessFlags & flags, const std::string_view & database);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void revoke(const AccessRightsElement & element);
    void revoke(const AccessRightsElements & elements);

    void tryRevoke(const AccessFlags & flags);
    void tryRevoke(const AccessFlags & flags, const std::string_view & database);
    void tryRevoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void tryRevoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void tryRevoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void tryRevoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void tryRevoke(const AccessRightsElement & element);
    void tryRevoke(const AccessRightsElements & elements);

    void revokeGrantOption(const AccessFlags & flags);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void revokeGrantOption(const AccessRightsElement & element);
    void revokeGrantOption(const AccessRightsElements & elements);

    /// Merges two sets of access rights together.
    /// It's used to combine access rights from multiple roles.
    void makeUnion(const AccessRightsBase & other);

    /// Makes an intersection of access rights.
    void makeIntersection(const AccessRightsBase & other);

    /// Traverse the tree and modify each access flags.
    using ModifyFlagsFunction = std::function<AccessFlags(
        const AccessFlags & flags,
        const AccessFlags & min_flags_with_children,
        const AccessFlags & max_flags_with_children,
        std::string_view database,
        std::string_view table,
        std::string_view column,
        bool grant_option)>;
    void modifyFlags(const ModifyFlagsFunction & function);

    struct Node;
    static bool sameNode(const std::unique_ptr<Node> & left, const std::unique_ptr<Node> & right);

    friend bool operator ==(AccessRightsBase const & left, AccessRightsBase const & right)
    {
        return sameNode(left.root, right.root) && sameNode(left.root_with_grant_option, right.root_with_grant_option);
    }
    friend bool operator !=(const AccessRightsBase & left, const AccessRightsBase & right) { return !(left == right); }

private:
    template <bool with_grant_option, typename... Args>
    void grantImpl(const AccessFlags & flags, int lvl, const Args &... args);

    template <bool with_grant_option>
    void grantImpl(const AccessRightsElement & element);

    template <bool with_grant_option>
    void grantImpl(const AccessRightsElements & elements);

    template <bool with_grant_option>
    void grantImplHelper(const AccessRightsElement & element);

    template <bool grant_option, bool is_exists, typename... Args>
    void revokeImpl(const AccessFlags & flags, const Args &... args);

    template <bool grant_option, bool is_exists>
    void revokeImpl(const AccessRightsElement & element);

    template <bool grant_option, bool is_exists>
    void revokeImpl(const AccessRightsElements & elements);

    template <bool grant_option, bool is_exists>
    void revokeImplHelper(const AccessRightsElement & element);


    void logTree() const;

protected:
    std::unique_ptr<Node> root;
    std::unique_ptr<Node> root_with_grant_option;
};

class AccessRights : public AccessRightsBase<false>
{
public:
    using Base = AccessRightsBase<false>;
    using Base::Base;
    bool isGranted(const AccessFlags & flags) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(const AccessRightsElement & element) const;
    bool isGranted(const AccessRightsElements & elements) const;

    bool hasGrantOption(const AccessFlags & flags) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool hasGrantOption(const AccessRightsElement & element) const;
    bool hasGrantOption(const AccessRightsElements & elements) const;

    /// Makes full access rights (GRANT ALL ON *.* WITH GRANT OPTION).
    static AccessRights getFullAccess();

private:
    template <bool grant_option, typename... Args>
    bool isGrantedImpl(const AccessFlags & flags, const Args &... args) const;

    template <bool grant_option>
    bool isGrantedImpl(const AccessRightsElement & element) const;

    template <bool grant_option>
    bool isGrantedImpl(const AccessRightsElements & elements) const;

    template <bool grant_option>
    bool isGrantedImplHelper(const AccessRightsElement & element) const;
};

class SensitiveAccessRights : public AccessRightsBase<true>
{
public:
    using Base = AccessRightsBase<true>;
    using Base::Base;
    bool isGranted(const std::unordered_set<std::string_view> & sensitive_columns, const AccessFlags & flags) const;
    bool isGranted(const std::unordered_set<std::string_view> & sensitive_columns, const AccessFlags & flags, const std::string_view & database) const;
    bool isGranted(const std::unordered_set<std::string_view> & sensitive_columns, const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(const std::unordered_set<std::string_view> & sensitive_columns, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(const std::unordered_set<std::string_view> & sensitive_columns, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const std::unordered_set<std::string_view> & sensitive_columns, const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(const std::unordered_set<std::string_view> & sensitive_columns, const AccessRightsElement & element) const;
    bool isGranted(const std::unordered_set<std::string_view> & sensitive_columns, const AccessRightsElements & elements) const;

private:
    template <bool grant_option, typename... Args>
    bool isGrantedImpl(const std::unordered_set<std::string_view> & sensitive_columns, const AccessFlags & flags, const Args &... args) const;

    template <bool grant_option>
    bool isGrantedImpl(const std::unordered_set<std::string_view> & sensitive_columns, const AccessRightsElement & element) const;

    template <bool grant_option>
    bool isGrantedImpl(const std::unordered_set<std::string_view> & sensitive_columns, const AccessRightsElements & elements) const;

    template <bool grant_option>
    bool isGrantedImplHelper(const std::unordered_set<std::string_view> & sensitive_columns, const AccessRightsElement & element) const;
};
}
