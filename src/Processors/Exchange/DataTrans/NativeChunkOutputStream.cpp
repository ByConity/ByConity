#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include "NativeChunkOutputStream.h"
#include <IO/VarInt.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


NativeChunkOutputStream::NativeChunkOutputStream(
    WriteBuffer & ostr_, UInt64 client_revision_, const Block & header_, bool remove_low_cardinality_)
    : ostr(ostr_), client_revision(client_revision_), header(header_), remove_low_cardinality(remove_low_cardinality_)
{
}

static void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column = column->convertToFullColumnIfConst();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0; //-V1048

    auto serialization = type.getDefaultSerialization();

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);
}


void NativeChunkOutputStream::write(const Chunk & chunk)
{
    /// Dimensions
    size_t columns = chunk.getNumColumns();
    size_t rows = chunk.getNumRows();

    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    for (size_t i = 0; i < columns; ++i)
    {
        DataTypePtr data_type = header.getDataTypes().at(i);
        ColumnPtr column_ptr = chunk.getColumns()[i];
        /// Send data to old clients without low cardinality type.
        if (remove_low_cardinality || (client_revision && client_revision < DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE))
        {
            column_ptr = recursiveRemoveLowCardinality(column_ptr);
            data_type = recursiveRemoveLowCardinality(data_type);
        }

        /// Name/Type, we don't need write name/type here.
        /// Data
        if (rows) /// Zero items of data is always represented as zero number of bytes.
            writeData(*data_type, column_ptr, ostr, 0, 0);
    }
}
}
