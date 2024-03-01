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

#include <iomanip>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query SHOW TABLES or SHOW DATABASES or SHOW CLUSTERS or SHOW SNAPSHOTS
  */
class ASTShowTablesQuery : public ASTQueryWithOutput
{
public:
    bool catalog{false};
    bool databases{false};
    bool clusters{false};
    bool cluster{false};
    bool dictionaries{false};
    bool snapshots{false};
    bool m_settings{false};
    bool changed{false};
    bool full{false};
    bool temporary{false};
    bool history{false};   // if set true, will show databases/tables in trash.
    bool external{false};
    String cluster_str;
    String from; // database name
    String from_catalog; // catalog name
    String like;

    bool not_like{false};
    bool case_insensitive_like{false};

    ASTPtr where_expression;
    ASTPtr limit_length;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ShowTables"; }

    ASTType getType() const override { return ASTType::ASTShowTablesQuery; }

    ASTPtr clone() const override;

    void toLowerCase() override 
    {
        boost::to_lower(cluster_str);
        boost::to_lower(from);
        boost::to_lower(from_catalog);
        boost::to_lower(like);
    }

    void toUpperCase() override 
    {
        boost::to_upper(cluster_str);
        boost::to_upper(from);
        boost::to_upper(from_catalog);
        boost::to_upper(like);
    }
protected:
    void formatLike(const FormatSettings & settings) const;
    void formatLimit(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
