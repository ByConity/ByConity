#include "NativeChunkInputStream.h"

#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
}

NativeChunkInputStream::NativeChunkInputStream(ReadBuffer & istr_, const Block & header_) : istr(istr_), header(header_)
{
}

void NativeChunkInputStream::readData(
    const IDataType & type, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.avg_value_size_hint = avg_value_size_hint;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    auto serialization = type.getDefaultSerialization();

    serialization->deserializeBinaryBulkStatePrefix(settings, state);
    serialization->deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw Exception(
            "Cannot read all data in NativeChunkInputStream. Rows read: " + toString(column->size()) + ". Rows expected: " + toString(rows)
                + ".",
            ErrorCodes::CANNOT_READ_ALL_DATA);
}

Chunk NativeChunkInputStream::readImpl()
{
    if (istr.eof())
    {
        return Chunk();
    }

    /// Dimensions
    size_t col_num = 0;
    size_t row_num = 0;

    readVarUInt(col_num, istr);
    readVarUInt(row_num, istr);

    Columns columns(col_num);
    for (size_t i = 0; i < col_num; ++i)
    {
        DataTypePtr data_type = header.getDataTypes().at(i);

        /// Data
        ColumnPtr read_column = data_type->createColumn();

        double avg_value_size_hint = avg_value_size_hints.empty() ? 0 : avg_value_size_hints[i];
        if (row_num) /// If no rows, nothing to read.
            readData(*data_type, read_column, istr, row_num, avg_value_size_hint);

        columns.push_back(read_column);
    }

    return Chunk(columns, row_num);
}
}
