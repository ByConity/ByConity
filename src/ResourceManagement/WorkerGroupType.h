#pragma once

#include <cctype>
#include <cstdint>
#include <cstring>

namespace DB::ResourceManagement
{
namespace WorkerGroupTypeImpl
{
    enum Type : uint8_t
    {
        Unknown = 0,
        Physical = 1,
        Shared = 2,
        Composite = 3,
    };
}
using WorkerGroupType = WorkerGroupTypeImpl::Type;

constexpr auto toString(WorkerGroupType type)
{
    switch (type)
    {
        case WorkerGroupType::Physical:
            return "Physical";
        case WorkerGroupType::Shared:
            return "Shared";
        case WorkerGroupType::Composite:
            return "Composite";
        default:
            return "Unknown";
    }
}

constexpr auto toWorkerGroupType(char * type_str)
{
    for (size_t i = 0; type_str[i]; ++i)
    {
        auto & c = type_str[i];
        if (i == 0)
            c = std::toupper(c);
        else
            c = std::tolower(c);
    }

    if (strcmp(type_str,  "Physical") == 0)
        return WorkerGroupType::Physical;
    else if (strcmp(type_str,  "Shared") == 0)
        return WorkerGroupType::Shared;
    else if (strcmp(type_str,  "Composite") == 0)
        return WorkerGroupType::Composite;
    else
        return WorkerGroupType::Unknown;

}

}
