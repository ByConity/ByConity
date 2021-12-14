#include <Core/SortDescription.h>
#include <Core/Block.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/JSONBuilder.h>

namespace DB
{

void SortColumnDescription::serialize(WriteBuffer & buffer) const
{
    writeBinary(column_name, buffer);
    writeBinary(column_number, buffer);
    writeBinary(direction, buffer);
    writeBinary(nulls_direction, buffer);
}

void SortColumnDescription::deserialize(ReadBuffer & buffer)
{
    readBinary(column_name, buffer);
    readBinary(column_number, buffer);
    readBinary(direction, buffer);
    readBinary(nulls_direction, buffer);
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

void serializeSortDescription(const SortDescription & sort_descriptions, WriteBuffer & buffer)
{
    writeBinary(sort_descriptions.size(), buffer);
    for (const auto & sort_description : sort_descriptions)
        sort_description.serialize(buffer);
}

void deserializeSortDescription(SortDescription & sort_descriptions, ReadBuffer & buffer)
{
    size_t sort_size;
    readBinary(sort_size, buffer);
    sort_descriptions.resize(sort_size);
    for (size_t i = 0; i < sort_size; ++i)
        sort_descriptions[i].deserialize(buffer);
}

}
