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

#include <Columns/Collator.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Protos/plan_node.pb.h>
#include <Common/JSONBuilder.h>

namespace DB
{

void FillColumnDescription::toProto(Protos::FillColumnDescription & proto) const
{
    fill_from.toProto(*proto.mutable_fill_from());
    fill_to.toProto(*proto.mutable_fill_to());
    fill_step.toProto(*proto.mutable_fill_step());
}

void FillColumnDescription::fillFromProto(const Protos::FillColumnDescription & proto)
{
    fill_from.fillFromProto(proto.fill_from());
    fill_to.fillFromProto(proto.fill_to());
    fill_step.fillFromProto(proto.fill_step());
}

void SortColumnDescription::toProto(Protos::SortColumnDescription & proto) const
{
    proto.set_column_name(column_name);
    proto.set_column_number(column_number);
    proto.set_direction(direction);
    proto.set_nulls_direction(nulls_direction);
    if (collator)
        proto.mutable_collator()->set_locale(collator->getLocale());
    proto.set_with_fill(with_fill);
    fill_description.toProto(*proto.mutable_fill_description());
}

void SortColumnDescription::fillFromProto(const Protos::SortColumnDescription & proto)
{
    column_name = proto.column_name();
    column_number = proto.column_number();
    direction = proto.direction();
    nulls_direction = proto.nulls_direction();
    if (proto.has_collator())
        collator = std::make_shared<Collator>(proto.collator().locale());
    with_fill = proto.with_fill();
    fill_description.fillFromProto(proto.fill_description());
}

void dumpSortDescription(const SortDescription & description, const Block & header, WriteBuffer & out)
{
    bool first = true;

    for (const auto & desc : description)
    {
        if (!first)
            out << ", ";
        first = false;

        if (!desc.column_name.empty())
            out << desc.column_name;
        else
        {
            if (desc.column_number < header.columns())
                out << header.getByPosition(desc.column_number).name;
            else
                out << "?";

            out << " (pos " << desc.column_number << ")";
        }

        if (desc.direction > 0)
            out << " ASC";
        else
            out << " DESC";

        if (desc.with_fill)
            out << " WITH FILL";
    }
}

void SortColumnDescription::explain(JSONBuilder::JSONMap & map, const Block & header) const
{
    if (!column_name.empty())
        map.add("Column", column_name);
    else
    {
        if (column_number < header.columns())
            map.add("Column", header.getByPosition(column_number).name);

        map.add("Position", column_number);
    }

    map.add("Ascending", direction > 0);
    map.add("With Fill", with_fill);
}

std::string dumpSortDescription(const SortDescription & description)
{
    WriteBufferFromOwnString wb;
    dumpSortDescription(description, Block{}, wb);
    return wb.str();
}

JSONBuilder::ItemPtr explainSortDescription(const SortDescription & description, const Block & header)
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & descr : description)
    {
        auto json_map = std::make_unique<JSONBuilder::JSONMap>();
        descr.explain(*json_map, header);
        json_array->add(std::move(json_map));
    }

    return json_array;
}

}
