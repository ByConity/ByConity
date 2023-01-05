#pragma once

#include <DataTypes/IDataType.h>
#include <Core/Names.h>

#include <unordered_map>

namespace DB
{

using NameToType = std::map<String, DataTypePtr>;

}
