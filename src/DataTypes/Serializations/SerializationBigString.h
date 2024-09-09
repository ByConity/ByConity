#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

/// This serialization method use multiple stream to serialize a string,
/// it will occupy one more extra byte for each string, since it will
/// store the tailing \0 into data file
class SerializationBigString final : public ISerialization
{
public:
    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinaryBulkWithMultipleStreams(const IColumn & column, size_t offset,
        size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;
    void deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const override;
    size_t deserializeBinaryBulkWithMultipleStreams(ColumnPtr & column, size_t limit,
        DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    bool supportMemComparableEncoding() const override { return true; }
    void serializeMemComparable(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeMemComparable(IColumn & column, ReadBuffer & istr) const override;

private:
    /// For encode string to guarantee value is ascending order in comparison.
    static constexpr UInt8 enc_group_size = 8;
    static constexpr char enc_marker = 0xFF;
    static constexpr char enc_pad = 0x00;
};

}
