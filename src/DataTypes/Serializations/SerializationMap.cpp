#include <common/map.h>
#include <common/logger_useful.h>

#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationTuple.h>

#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_MAP_FROM_TEXT;
}

SerializationMap::SerializationMap(const SerializationPtr & key_, const SerializationPtr & value_, const SerializationPtr & nested_)
    : key(key_), value(value_), nested(nested_)
{
}

static const IColumn & extractNestedColumn(const IColumn & column)
{
    return assert_cast<const ColumnMap &>(column).getNestedColumn();
}

static IColumn & extractNestedColumn(IColumn & column)
{
    return assert_cast<ColumnMap &>(column).getNestedColumn();
}

void SerializationMap::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & map = get<const Map &>(field);
    writeVarUInt(map.size(), ostr);
    for (const auto & elem : map)
    {
        key->serializeBinary(elem.first, ostr);
        value->serializeBinary(elem.second, ostr);
    }
}

void SerializationMap::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    size_t size;
    readVarUInt(size, istr);
    field = Map(size);
    for (auto & elem : field.get<Map &>())
    {
        key->deserializeBinary(elem.first, istr);
        value->deserializeBinary(elem.second, istr);
    }
}

void SerializationMap::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    nested->serializeBinary(extractNestedColumn(column), row_num, ostr);
}

void SerializationMap::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    nested->deserializeBinary(extractNestedColumn(column), istr);
}


template <typename KeyWriter, typename ValueWriter>
void SerializationMap::serializeTextImpl(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    KeyWriter && key_writer,
    ValueWriter && value_writer) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & offsets = column_map.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    writeChar('{', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);

        key_writer(ostr, key, column_map.getKey(), i);
        writeChar(':', ostr);
        value_writer(ostr, value, column_map.getValue(), i);
    }
    writeChar('}', ostr);
}

template <typename Reader>
void SerializationMap::deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && reader, const FormatSettings & settings) const
{
    auto & column_map = assert_cast<ColumnMap &>(column);
    auto & offsets = column_map.getOffsets();
    auto & key_column = column_map.getKey();
    auto & value_column = column_map.getValue();

    if (settings.map.parse_null_map_as_empty && checkStringByFirstCharacterAndAssertTheRest("null", istr))
    {
        /// Still push a zero-length offset
        offsets.push_back((offsets.empty() ? 0 : offsets.back()) + 0);
        /// Early return if got a null
        return;
    }

    if (settings.map.parse_null_map_as_empty && checkStringByFirstCharacterAndAssertTheRest("null", istr))
    {
        /// Still push a zero-length offset
        offsets.push_back((offsets.empty() ? 0 : offsets.back()) + 0);
        /// Early return if got a null
        return;
    }

    size_t ksize = 0, vsize = 0;
    assertChar('{', istr);

    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != '}')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                    ++istr.position();
                else
                    throw Exception("Cannot read Map from text", ErrorCodes::CANNOT_READ_MAP_FROM_TEXT);
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == '}')
                break;

            reader(istr, key, key_column);
            ksize++;

            skipWhitespaceIfAny(istr);

            assertChar(':', istr);

            skipWhitespaceIfAny(istr);

            if (settings.map.skip_null_map_value && checkStringByFirstCharacterAndAssertTheRest("null", istr))
            {
                /// Pop the key read just now
                key_column.popBack(1);
                ksize--;
            }
            else
            {
                reader(istr, value, value_column);
                vsize++;

                /// Check the length of key after got key and value
                if (auto && current_key = key_column.getDataAt(key_column.size() - 1);
                    unlikely(current_key.size > settings.map.max_map_key_length))
                {
                    LOG_WARNING(
                        getLogger("SerializationMap"),
                        "Key of map can not be longer than {}, discard key: {}",
                        settings.map.max_map_key_length,
                        current_key.toString());

                    /// Pop the long key with value
                    key_column.popBack(1);
                    ksize--;
                    value_column.popBack(1);
                    vsize--;
                }
            }

            skipWhitespaceIfAny(istr);
        }

        assertChar('}', istr);
    }
    catch (...)
    {
        if (ksize)
        {
            key_column.popBack(ksize);
        }
        if (vsize)
        {
            value_column.popBack(vsize);
        }
        throw;
    }

    // ksize and vsize should be the same here, use anyone
    offsets.push_back((offsets.empty() ? 0 : offsets.back()) + ksize);
}

void SerializationMap::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto writer = [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
    {
        subcolumn_serialization->serializeTextQuoted(subcolumn, pos, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, writer, writer);
}

void SerializationMap::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto reader = [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn) {
        subcolumn_serialization->deserializeTextQuoted(subcolumn, buf, settings);
    };

    deserializeTextImpl(column, istr, reader, settings);
}

void SerializationMap::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
        {
            /// We need to double-quote all keys (including integers) to produce valid JSON.
            WriteBufferFromOwnString str_buf;
            subcolumn_serialization->serializeText(subcolumn, pos, str_buf, settings);
            writeJSONString(str_buf.str(), buf, settings);
        },
        [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
        {
            subcolumn_serialization->serializeTextJSON(subcolumn, pos, buf, settings);
        });
}

void SerializationMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto reader = [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn) {
        subcolumn_serialization->deserializeTextJSON(subcolumn, buf, settings);
    };

    deserializeTextImpl(column, istr, reader, settings);
}

void SerializationMap::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & offsets = column_map.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const auto & key_column = column_map.getKey();
    const auto & value_column = column_map.getValue();

    writeCString("<map>", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        writeCString("<elem>", ostr);
        writeCString("<key>", ostr);
        key->serializeTextXML(key_column, i, ostr, settings);
        writeCString("</key>", ostr);

        writeCString("<value>", ostr);
        value->serializeTextXML(value_column, i, ostr, settings);
        writeCString("</value>", ostr);
        writeCString("</elem>", ostr);
    }
    writeCString("</map>", ostr);
}

void SerializationMap::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}

void SerializationMap::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);
    deserializeText(column, rb, settings);
}

void SerializationMap::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    auto next_data = SubstreamData(nested)
        .withType(data.type ? assert_cast<const DataTypeMap &>(*data.type).getNestedType() : nullptr)
        .withColumn(data.column ? assert_cast<const ColumnMap &>(*data.column).getNestedColumnPtr() : nullptr)
        .withSerializationInfo(data.serialization_info);

    nested->enumerateStreams(settings, callback, next_data);
}

void SerializationMap::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(extractNestedColumn(column), settings, state);
}

void SerializationMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state);
}


void SerializationMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), offset, limit, settings, state);
}

size_t SerializationMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto & column_map = assert_cast<ColumnMap &>(*column->assumeMutable());
    return nested->deserializeBinaryBulkWithMultipleStreams(column_map.getNestedColumnPtr(), limit, settings, state, cache);
}

}
