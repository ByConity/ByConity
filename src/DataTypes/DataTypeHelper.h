#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

DataTypePtr createBaseDataTypeFromTypeIndex(TypeIndex index);

void serializeDataTypeV1(const DataTypePtr & data_type, WriteBuffer & buf);
DataTypePtr deserializeDataTypeV1(ReadBuffer & buf);

void serializeDataType(const DataTypePtr & data_type, WriteBuffer & buf);
DataTypePtr deserializeDataType(ReadBuffer & buf);

void serializeDataTypes(const DataTypes & data_types, WriteBuffer & buf);
DataTypes deserializeDataTypes(ReadBuffer & buf);

}
