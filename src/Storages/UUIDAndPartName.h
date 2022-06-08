#pragma once

#include <string>
#include <utility>

#include <city.h>
#include <Core/UUID.h>

namespace DB
{
using UUIDAndPartName = std::pair<UUID, std::string>;

struct UUIDAndPartNameHash
{
    size_t operator()(const UUIDAndPartName & key) const
    {
        return UInt128Hash{}(key.first) ^ CityHash_v1_0_2::CityHash64(key.second.data(), key.second.length());
    }
};

using TableWithPartitionHash = UUIDAndPartNameHash;

}
