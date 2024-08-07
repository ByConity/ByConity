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

#include <vector>
#include <memory>
#include <cstddef>
#include <string>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>

class Collator;

namespace DB
{

namespace Protos
{
    class FillColumnDescription;
    class SortColumnDescription;
    class SortDescription;
}

namespace JSONBuilder
{
    class JSONMap;
    class IItem;
    using ItemPtr = std::unique_ptr<IItem>;
}

class Block;
class WriteBuffer;
class ReadBuffer;

struct FillColumnDescription
{
    /// All missed values in range [FROM, TO) will be filled
    /// Range [FROM, TO) respects sorting direction
    Field fill_from;        /// Fill value >= FILL_FROM
    Field fill_to;          /// Fill value + STEP < FILL_TO
    Field fill_step;        /// Default = 1 or -1 according to direction

    void toProto(Protos::FillColumnDescription & proto) const;
    void fillFromProto(const Protos::FillColumnDescription & proto);
};

/// Description of the sorting rule by one column.
struct SortColumnDescription
{
    std::string column_name; /// The name of the column.
    size_t column_number;    /// Column number (used if no name is given).
    int direction;           /// 1 - ascending, -1 - descending.
    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
                             /// To achieve NULLS LAST, set it equal to direction, to achieve NULLS FIRST, set it opposite.
    std::shared_ptr<Collator> collator = nullptr; /// Collator for locale-specific comparison of strings
    bool with_fill = false;
    FillColumnDescription fill_description = {};

    SortColumnDescription(
            size_t column_number_, int direction_, int nulls_direction_,
            const std::shared_ptr<Collator> & collator_ = nullptr,
            bool with_fill_ = false, const FillColumnDescription & fill_description_ = {})
            : column_number(column_number_), direction(direction_), nulls_direction(nulls_direction_), collator(collator_)
            , with_fill(with_fill_), fill_description(fill_description_) {}

    SortColumnDescription(
            const std::string & column_name_, int direction_, int nulls_direction_,
            const std::shared_ptr<Collator> & collator_ = nullptr,
            bool with_fill_ = false, const FillColumnDescription & fill_description_ = {})
            : column_name(column_name_), column_number(0), direction(direction_), nulls_direction(nulls_direction_)
            , collator(collator_), with_fill(with_fill_), fill_description(fill_description_) {}

    SortColumnDescription() {}

    bool operator == (const SortColumnDescription & other) const
    {
        return column_name == other.column_name && column_number == other.column_number && direction == other.direction
            && nulls_direction == other.nulls_direction;
    }

    bool operator != (const SortColumnDescription & other) const
    {
        return !(*this == other);
    }

    std::string dump() const
    {
        return fmt::format("{}:{}:dir {}nulls ", column_name, column_number, direction, nulls_direction);
    }

    std::string format() const
    {
        return fmt::format(
            "{} {} {}", 
            column_name, 
            direction == 1 ? "ASC" : "DESC", 
            nulls_direction == 0 ? "ANY" : 
                (nulls_direction == direction ? "NULLS LAST" : "NULLS FIRST"));
    }

    void explain(JSONBuilder::JSONMap & map, const Block & header) const;

    /// It seems that the current construction of SortColumnDescription only uses the first four fields,
    /// so this time will temporarily ignore the serialize/deserialize of field collator/with_fill/fill_description

    void toProto(Protos::SortColumnDescription & proto) const;
    void fillFromProto(const Protos::SortColumnDescription & proto);
};

/// Description of the sorting rule for several columns.
class SortDescription : public std::vector<SortColumnDescription>
{
public:
    using vector::vector;
};

/// Outputs user-readable description into `out`.
void dumpSortDescription(const SortDescription & description, const Block & header, WriteBuffer & out);

std::string dumpSortDescription(const SortDescription & description);

JSONBuilder::ItemPtr explainSortDescription(const SortDescription & description, const Block & header);

}
