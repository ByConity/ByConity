#pragma once
#include <Storages/Hive/HiveDataPart_fwd.h>

namespace DB
{

struct RowGroupsInDataPart
{
    HiveDataPartCNCHPtr data_part;
    size_t total_row_groups;

    RowGroupsInDataPart() = default;

    RowGroupsInDataPart(const HiveDataPartCNCHPtr & data_part_, size_t total_row_groups_ = 0)
        : data_part(data_part_)
        , total_row_groups(total_row_groups_)
    {}

    size_t getRowGroups() const
    {
        return total_row_groups;
    }

};

using RowGroupsInDataParts = std::vector<RowGroupsInDataPart>;
}
