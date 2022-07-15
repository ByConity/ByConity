#pragma once

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>

namespace DB
{
class Context;
}

namespace DB::CnchPartsHelper
{

enum LoggingOption
{
    DisableLogging = 0,
    EnableLogging = 1,
};

LoggingOption getLoggingOption(const Context & c);

MergeTreeDataPartsVector calcVisibleParts(MergeTreeDataPartsVector & all_parts, bool flatten, LoggingOption logging = DisableLogging);
ServerDataPartsVector calcVisibleParts(ServerDataPartsVector & all_parts, bool flatten, LoggingOption logging = DisableLogging);

ServerDataPartsVector calcVisiblePartsForGC(
    ServerDataPartsVector & all_parts,
    ServerDataPartsVector * visible_alone_drop_ranges,
    ServerDataPartsVector * invisible_dropped_parts,
    LoggingOption logging = DisableLogging);

template <typename T>
void flattenPartsVector(std::vector<T> & visible_parts)
{
    size_t size = visible_parts.size();
    for (size_t i = 0; i < size; ++i)
    {
        auto prev_part = visible_parts[i]->tryGetPreviousPart();
        while (prev_part)
        {
            if constexpr (std::is_same_v<T, decltype(prev_part)>)
                visible_parts.push_back(prev_part);
            else
                visible_parts.push_back(std::dynamic_pointer_cast<typename T::value_type>(prev_part));

            prev_part = prev_part->tryGetPreviousPart();
        }
    }
}
}
