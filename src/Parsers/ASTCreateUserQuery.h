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

#pragma once

#include <Parsers/IAST.h>
// #include <Parsers/ASTQueryWithOnCluster.h>
#include <Access/Authentication.h>
// #include <Access/AllowedClientHosts.h>


namespace DB
{
class ASTUserNamesWithHost;
class ASTRolesOrUsersSet;
class ASTSettingsProfileElements;
class Context;

/** CREATE USER [IF NOT EXISTS | OR REPLACE] name
  *     [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}}|{WITH ldap SERVER 'server_name'}|{WITH kerberos [REALM 'realm']}]
  *     [DEFAULT ROLE role [,...]]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *     [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
  *
  * ALTER USER [IF EXISTS] name
  *     [RENAME TO new_name]
  *     [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password|plaintext_password|sha256_password|sha256_hash|double_sha1_password|double_sha1_hash}] BY {'password'|'hash'}}|{WITH ldap SERVER 'server_name'}|{WITH kerberos [REALM 'realm']}]
  *     [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
  *     [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
  *     [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
  */
class ASTCreateUserQuery : public IAST
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    std::shared_ptr<ASTUserNamesWithHost> names;
    String new_name;

    std::optional<Authentication> authentication;
    bool show_password = true; /// formatImpl() will show the password or hash.
    bool tenant_rewritten = false;

    // std::optional<AllowedClientHosts> hosts;
    // std::optional<AllowedClientHosts> add_hosts;
    // std::optional<AllowedClientHosts> remove_hosts;

    std::shared_ptr<ASTRolesOrUsersSet> default_roles;
    std::shared_ptr<ASTSettingsProfileElements> settings;
    std::shared_ptr<ASTRolesOrUsersSet> grantees;

    String getID(char) const override;

    ASTType getType() const override { return ASTType::ASTCreateUserQuery; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
    void rewriteUserNameWithTenant(const Context *);
    // ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateUserQuery>(clone()); }
};
}
