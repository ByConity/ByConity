#pragma once

#include <cctype>
#include <cstdint>
#include <cstring>
#include <vector>
#include <Core/Types.h>

namespace DB::ResourceManagement
{
namespace VirtualWarehouseTypeImpl
{
    enum Type : uint8_t
    {
        Unknown = 0,
        Read = 1,
        Write = 2,
        Task = 3,
        Default = 4,
        Num = 5,
    };
}
using VirtualWarehouseType = VirtualWarehouseTypeImpl::Type;
using VirtualWarehouseTypes = std::vector<VirtualWarehouseType>;

constexpr auto toString(VirtualWarehouseType type)
{
    switch (type)
    {
        case VirtualWarehouseType::Read:
            return "Read";
        case VirtualWarehouseType::Write:
            return "Write";
        case VirtualWarehouseType::Task:
            return "Task";
        case VirtualWarehouseType::Default:
            return "Default";
        default:
            return "Unknown";
    }
}

constexpr VirtualWarehouseType toVirtualWarehouseType(char * type)
{
    for (size_t i = 0; type[i]; ++i)
    {
        auto & c = type[i];
        if (i == 0)
            c = std::toupper(c);
        else
            c = std::tolower(c);
    }

    if (strcmp(type, "Read") == 0)
        return VirtualWarehouseType::Read;
    else if (strcmp(type,"Write") == 0)
        return VirtualWarehouseType::Write;
    else if (strcmp(type,"Task") == 0)
        return VirtualWarehouseType::Task;
    else if (strcmp(type,"Default") == 0)
        return VirtualWarehouseType::Default;
    else
        return VirtualWarehouseType::Unknown;
}

//TODO Remove when system VW logic is deprecated
constexpr auto toSystemVWName(VirtualWarehouseType t)
{
    switch (t)
    {
        case VirtualWarehouseType::Read:
            return "vw_read";
        case VirtualWarehouseType::Write:
            return "vw_write";
        case VirtualWarehouseType::Task:
            return "vw_task";
        case VirtualWarehouseType::Default:
            return "vw_default";
        default:
            return "";
    }

}

bool isSystemVW(const String & virtual_warehouse);

}
