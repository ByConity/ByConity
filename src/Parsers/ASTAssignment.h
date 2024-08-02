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

namespace DB
{

/// Part of the ALTER UPDATE statement of the form: column = expr or tbl_alias.col = expr
class ASTAssignment : public IAST
{
public:
    String table_name;
    String column_name;

    ASTPtr expression() const
    {
        return children.at(0);
    }

    String tablePrefix() const { return table_name.empty() ? "" : table_name + "."; }

    String getID(char delim) const override { return "Assignment" + (delim + tablePrefix() + column_name); }

    ASTType getType() const override { return ASTType::ASTAssignment; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTAssignment>(*this);
        res->children = { expression()->clone() };
        return res;
    }

    void toLowerCase() override 
    {
        boost::to_lower(table_name);
        boost::to_lower(column_name);
    }

    void toUpperCase() override 
    {
        boost::to_upper(table_name);
        boost::to_upper(column_name);
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_identifier : "");
        if (!table_name.empty())
        {
            settings.writeIdentifier(table_name);
            settings.ostr << ".";
        }
        settings.writeIdentifier(column_name);
        settings.ostr << (settings.hilite ? hilite_none : "");

        settings.ostr << (settings.hilite ? hilite_operator : "") << " = " << (settings.hilite ? hilite_none : "");

        expression()->formatImpl(settings, state, frame);
    }
};

}
