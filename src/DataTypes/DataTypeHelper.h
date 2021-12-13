#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

DataTypePtr createBaseDataTypeFromTypeIndex(TypeIndex index);

void serializeDataType(const DataTypePtr & data_type, WriteBuffer & buf);
DataTypePtr deserializeDataType(ReadBuffer & buf);

}
