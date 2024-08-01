/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <DataTypes/Serializations/ISerialization.h>
#include <Columns/IColumn.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/NestedUtils.h>
#include <magic_enum.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
    extern const int NOT_IMPLEMENTED;
}

ISerialization::Kind ISerialization::getKind(const IColumn & column)
{
    if (column.isSparse())
        return Kind::SPARSE;

    return Kind::DEFAULT;
}

String ISerialization::kindToString(Kind kind)
{
    switch (kind)
    {
        case Kind::DEFAULT:
            return "Default";
        case Kind::SPARSE:
            return "Sparse";
    }
    UNREACHABLE();
}

ISerialization::Kind ISerialization::stringToKind(const String & str)
{
    if (str == "Default")
        return Kind::DEFAULT;
    else if (str == "Sparse")
        return Kind::SPARSE;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown serialization kind '{}'", str);
}

String ISerialization::Substream::toString() const
{
    if (type == TupleElement)
        return fmt::format("TupleElement({}, escape_tuple_delimiter = {})", tuple_element_name, escape_tuple_delimiter ? "true" : "false");

    return String(magic_enum::enum_name(type));
}

String ISerialization::SubstreamPath::toString() const
{
    WriteBufferFromOwnString wb;
    wb << "{";
    for (size_t i = 0; i < size(); ++i)
    {
        if (i != 0)
            wb << ", ";
        wb << at(i).toString();
    }
    wb << "}";
    return wb.str();
}

void ISerialization::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    settings.path.push_back(Substream::Regular);
    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

void ISerialization::enumerateStreams(
    const StreamCallback & callback,
    const DataTypePtr & type,
    const ColumnPtr & column) const
{
    EnumerateStreamsSettings settings;
    auto data = SubstreamData(getPtr()).withType(type).withColumn(column);
    enumerateStreams(settings, callback, data);
}

void ISerialization::enumerateStreams(
        const StreamCallback & callback,
        const SubstreamPath & path,
        const DataTypePtr & type,
        const ColumnPtr & column) const
{
    EnumerateStreamsSettings settings;
    settings.path = path;
    auto data = SubstreamData(getPtr()).withType(type).withColumn(column);
    enumerateStreams(settings, callback, data);
}

void ISerialization::serializeBinaryBulk(const IColumn & column, WriteBuffer &, size_t, size_t) const
{
    throw Exception(ErrorCodes::MULTIPLE_STREAMS_REQUIRED, "Column {} must be serialized with multiple streams", column.getName());
}

void ISerialization::deserializeBinaryBulk(IColumn & column, ReadBuffer &, size_t, double, bool) const
{
    throw Exception(ErrorCodes::MULTIPLE_STREAMS_REQUIRED, "Column {} must be deserialized with multiple streams", column.getName());
}

void ISerialization::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & /* state */) const
{
    if (WriteBuffer * stream = settings.getter(settings.path))
        serializeBinaryBulk(column, *stream, offset, limit);
}

void ISerialization::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    auto cached_column = getFromSubstreamsCache(cache, settings.path);
    if (cached_column)
    {
        column = cached_column;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        auto mutable_column = column->assumeMutable();
        deserializeBinaryBulk(*mutable_column, *stream, limit, settings.avg_value_size_hint, settings.zero_copy_read_from_cache);
        column = std::move(mutable_column);
        addToSubstreamsCache(cache, settings.path, column);
    }
}

using SubstreamIterator = ISerialization::SubstreamPath::const_iterator;

size_t ISerialization::skipBinaryBulkWithMultipleStreams(
    const NameAndTypePair & name_and_type,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    ColumnPtr tmp_column = name_and_type.type->createColumn();
    deserializeBinaryBulkWithMultipleStreams(tmp_column, limit, settings, state, cache);
    return tmp_column->size();
}

static String getNameForSubstreamPath(
    String stream_name,
    SubstreamIterator begin,
    SubstreamIterator end,
    bool escape_tuple_delimiter)
{
    using Substream = ISerialization::Substream;

    size_t array_level = 0;
    size_t null_level = 0;
    for (auto it = begin; it != end; ++it)
    {
        if (it->type == Substream::NullMap)
            stream_name += ".null" + (null_level > 0 ? toString(null_level): "");
        else if (it->type == Substream::ArraySizes)
            stream_name += ".size" + toString(array_level);
        else if (it->type == Substream::ArrayElements)
            ++array_level;
        else if (it->type == Substream::NullableElements)
            ++null_level;
        else if (it->type == Substream::DictionaryKeys)
            stream_name += ".dict";
        else if (it->type == Substream::SparseOffsets)
            stream_name += ".sparse.idx";
        else if (it->type == Substream::TupleElement)
        {
            /// For compatibility reasons, we use %2E (escaped dot) instead of dot.
            /// Because nested data may be represented not by Array of Tuple,
            ///  but by separate Array columns with names in a form of a.b,
            ///  and name is encoded as a whole.
            if (escape_tuple_delimiter && it->escape_tuple_delimiter)
                stream_name += escapeForFileName("." + it->tuple_element_name);
            else
                stream_name += "." + it->tuple_element_name;
        }
        else if (it->type == Substream::StringElements)
            stream_name += ".str_elements";
        else if (it->type == Substream::StringOffsets)
            stream_name += ".str_offsets";
    }

    return stream_name;
}

String ISerialization::getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path)
{
    return getFileNameForStream(column.getNameInStorage(), path);
}

String ISerialization::getFileNameForStream(const String & name_in_storage, const SubstreamPath & path)
{
    String stream_name;
    if (isSharedOffsetSubstream(name_in_storage, path))
        stream_name = escapeForFileName(Nested::extractTableName(name_in_storage));
    else
        stream_name = escapeForFileName(name_in_storage);

    return getNameForSubstreamPath(std::move(stream_name), path.begin(), path.end(), true);
}

String ISerialization::getSubcolumnNameForStream(const SubstreamPath & path)
{
    return getSubcolumnNameForStream(path, path.size());
}

String ISerialization::getSubcolumnNameForStream(const SubstreamPath & path, size_t prefix_len)
{
    auto subcolumn_name = getNameForSubstreamPath("", path.begin(), path.begin() + prefix_len, false);
    if (!subcolumn_name.empty())
        subcolumn_name = subcolumn_name.substr(1); // It starts with a dot.

    return subcolumn_name;
}

void ISerialization::addToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path, ColumnPtr column)
{
    if (cache && !path.empty())
        cache->emplace(getSubcolumnNameForStream(path), column);
}

ColumnPtr ISerialization::getFromSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path)
{
    if (!cache || path.empty())
        return nullptr;

    auto it = cache->find(getSubcolumnNameForStream(path));
    if (it == cache->end())
        return nullptr;

    return it->second;
}

bool ISerialization::isSpecialCompressionAllowed(const SubstreamPath & path)
{
    for (const auto & elem : path)
    {
        if (elem.type == Substream::NullMap
            || elem.type == Substream::ArraySizes
            || elem.type == Substream::DictionaryIndexes
            || elem.type == Substream::SparseOffsets)
            return false;
    }
    return true;
}

bool ISerialization::isSharedOffsetSubstream(const String & name_in_storage, const SubstreamPath & path)
{
    auto nested_storage_name = Nested::extractTableName(name_in_storage);
    return name_in_storage != nested_storage_name && (path.size() == 1 && path[0].type == ISerialization::Substream::ArraySizes);
}

void ISerialization::serializeMemComparable(const IColumn &, size_t, WriteBuffer &) const
{
    throw Exception("Serialization type doesn't support mem-comparable encoding", ErrorCodes::NOT_IMPLEMENTED);
}

void ISerialization::deserializeMemComparable(IColumn &, ReadBuffer &) const
{
    throw Exception("Serialization type doesn't support mem-comparable encoding", ErrorCodes::NOT_IMPLEMENTED);
}

size_t ISerialization::getArrayLevel(const SubstreamPath & path)
{
    size_t level = 0;
    for (const auto & elem : path)
        level += elem.type == Substream::ArrayElements;
    return level;
}

bool ISerialization::hasSubcolumnForPath(const SubstreamPath & path, size_t prefix_len)
{
    if (prefix_len == 0 || prefix_len > path.size())
        return false;

    size_t last_elem = prefix_len - 1;
    return path[last_elem].type == Substream::NullMap
            || path[last_elem].type == Substream::TupleElement
            || path[last_elem].type == Substream::ArraySizes;
}

ISerialization::SubstreamData ISerialization::createFromPath(const SubstreamPath & path, size_t prefix_len)
{
    assert(prefix_len <= path.size());
    if (prefix_len == 0)
        return {};

    ssize_t last_elem = prefix_len - 1;
    auto res = path[last_elem].data;
    for (ssize_t i = last_elem - 1; i >= 0; --i)
    {
        const auto & creator = path[i].creator;
        if (creator)
        {
            res.type = res.type ? creator->create(res.type) : res.type;
            res.serialization = res.serialization ? creator->create(res.serialization) : res.serialization;
            res.column = res.column ? creator->create(res.column) : res.column;
        }
    }

    return res;
}

void ISerialization::throwUnexpectedDataAfterParsedValue(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const String & type_name) const
{
    WriteBufferFromOwnString ostr;
    serializeText(column, column.size() - 1, ostr, settings);
    throw Exception(
        ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
        "Unexpected data '{}' after parsed {} value '{}'",
        std::string(istr.position(), std::min(size_t(10), istr.available())),
        type_name,
        ostr.str());
}

}
