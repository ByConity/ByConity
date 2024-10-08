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
#include <Parsers/ASTQueryWithOutput.h>
#include <Core/UUID.h>


namespace DB
{

struct StorageID;
/** Query specifying table name and, possibly, the database and the FORMAT section.
  */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    String catalog;
    String database;
    String table;
    UUID uuid = UUIDHelpers::Nil;
    bool temporary{false};

    String getDatabase() const;
    String getTable() const;

    void setDatabase(const String & name);
    void setTable(const String & name);

    ASTType getType() const override { return ASTType::ASTQueryWithTableAndOutput; }
    void setTableInfo(const StorageID& storage_id);
    StorageID getTableInfo() const;

    void toLowerCase() override
    {
        boost::to_lower(catalog);
        boost::to_lower(database);
        boost::to_lower(table);
    }
    void toUpperCase() override
    {
        boost::to_upper(catalog);
        boost::to_upper(database);
        boost::to_upper(table);
    }
protected:
    void formatHelper(const FormatSettings & settings, const char * name) const;
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const override { return AstIDAndQueryNames::ID + (delim + database) + delim + table; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        formatHelper(settings, temporary ? AstIDAndQueryNames::QueryTemporary : AstIDAndQueryNames::Query);
    }
};

}
