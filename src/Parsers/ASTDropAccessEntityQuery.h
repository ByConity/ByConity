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
#include <Access/RowPolicy.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{
class ASTRowPolicyNames;
class Context;

/** DROP USER [IF EXISTS] name [,...]
  * DROP ROLE [IF EXISTS] name [,...]
  * DROP QUOTA [IF EXISTS] name [,...]
  * DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...]
  * DROP [SETTINGS] PROFILE [IF EXISTS] name [,...]
  */
class ASTDropAccessEntityQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    using EntityType = IAccessEntity::Type;

    EntityType type;
    bool if_exists = false;
    Strings names;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;
    bool tenant_rewritten = false;

    String getID(char) const override;

    ASTType getType() const override { return ASTType::ASTDropAccessEntityQuery; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void rewriteNamesWithTenant(const Context *);
    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTDropAccessEntityQuery>(clone()); }

    void replaceEmptyDatabase(const String & current_database) const;
};
}
