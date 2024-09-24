#include "Storages/Hive/HiveVirtualColumns.h"

#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypeLowCardinality.h"
#include "DataTypes/DataTypesNumber.h"

namespace DB
{

NamesAndTypesList getHiveVirtuals()
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_size", makeNullable(std::make_shared<DataTypeUInt64>())}};
}

void eraseHiveVirtuals(Block & block)
{
    if (block.has("_path"))
        block.erase("_path");
    if (block.has("_file"))
        block.erase("_file");
    if (block.has("_size"))
        block.erase("_size");
}

}
