#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

class SerializationTupleElement final : public SerializationWrapper
{
private:
    String name;
    bool escape_delimiter;

public:
    SerializationTupleElement(const SerializationPtr & nested_, const String & name_, bool escape_delimiter_ = true)
        : SerializationWrapper(nested_)
        , name(name_), escape_delimiter(escape_delimiter_)
    {
    }

    const String & getElementName() const { return name; }

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    size_t deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

private:
    void addToPath(SubstreamPath & path) const;
};

}
