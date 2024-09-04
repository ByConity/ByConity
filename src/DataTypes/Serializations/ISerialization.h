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

#pragma once

#include <Common/COW.h>
#include <Core/Types.h>
#include <Columns/IColumn.h>

#include <boost/noncopyable.hpp>
#include <unordered_map>
#include <memory>

namespace DB
{

class IDataType;

class ReadBuffer;
class WriteBuffer;
class ProtobufReader;
class ProtobufWriter;

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class ISerialization;
using SerializationPtr = std::shared_ptr<const ISerialization>;

class SerializationInfo;
using SerializationInfoPtr = std::shared_ptr<const SerializationInfo>;

class Field;

struct FormatSettings;
struct NameAndTypePair;

class CompressedDataIndex;

class ISerialization : private boost::noncopyable, public std::enable_shared_from_this<ISerialization>
{
public:
    ISerialization() = default;
    virtual ~ISerialization() = default;

    enum class Kind : UInt8
    {
        DEFAULT = 0,
        SPARSE = 1,
    };

    SerializationPtr getPtr() const { return shared_from_this(); }

    static Kind getKind(const IColumn & column);
    static String kindToString(Kind kind);
    static Kind stringToKind(const String & str);

    /** Binary serialization for range of values in column - for writing to disk/network, etc.
      *
      * Some data types are represented in multiple streams while being serialized.
      * Example:
      * - Arrays are represented as stream of all elements and stream of array sizes.
      * - Nullable types are represented as stream of values (with unspecified values in place of NULLs) and stream of NULL flags.
      *
      * Different streams are identified by "path".
      * If the data type require single stream (it's true for most of data types), the stream will have empty path.
      * Otherwise, the path can have components like "array elements", "array sizes", etc.
      *
      * For multidimensional arrays, path can have arbitrary length.
      * As an example, for 2-dimensional arrays of numbers we have at least three streams:
      * - array sizes;                      (sizes of top level arrays)
      * - array elements / array sizes;     (sizes of second level (nested) arrays)
      * - array elements / array elements;  (the most deep elements, placed contiguously)
      *
      * Descendants must override either serializeBinaryBulk, deserializeBinaryBulk methods (for simple cases with single stream)
      *  or serializeBinaryBulkWithMultipleStreams, deserializeBinaryBulkWithMultipleStreams, enumerateStreams methods (for cases with multiple streams).
      *
      * Default implementations of ...WithMultipleStreams methods will call serializeBinaryBulk, deserializeBinaryBulk for single stream.
      */
    struct ISubcolumnCreator
    {
        virtual DataTypePtr create(const DataTypePtr & prev) const = 0;
        virtual SerializationPtr create(const SerializationPtr & prev) const = 0;
        virtual ColumnPtr create(const ColumnPtr & prev) const = 0;
        virtual ~ISubcolumnCreator() = default;
    };

    using SubcolumnCreatorPtr = std::shared_ptr<const ISubcolumnCreator>;

    struct SubstreamData
    {
        SubstreamData() = default;
        SubstreamData(SerializationPtr serialization_)
            : serialization(std::move(serialization_))
        {
        }

        SubstreamData & withType(DataTypePtr type_)
        {
            type = std::move(type_);
            return *this;
        }

        SubstreamData & withColumn(ColumnPtr column_)
        {
            column = std::move(column_);
            return *this;
        }

        SubstreamData & withSerializationInfo(SerializationInfoPtr serialization_info_)
        {
            serialization_info = std::move(serialization_info_);
            return *this;
        }

        SerializationPtr serialization;
        DataTypePtr type;
        ColumnPtr column;
        SerializationInfoPtr serialization_info;
    };

    struct Substream
    {
        enum Type
        {
            ArrayElements,
            ArraySizes,

            NullableElements,
            NullMap,

            TupleElement,

            DictionaryKeys,
            DictionaryIndexes,

            SparseElements,
            SparseOffsets,

            ObjectStructure,
            ObjectData,

            Regular,
            StringElements,
            StringOffsets,
        };
        Type type;

        /// Index of tuple element, starting at 1 or name.
        String tuple_element_name;

        /// Name of subcolumn of object column.
        String object_key_name;

        /// Do we need to escape a dot in filenames for tuple elements.
        bool escape_tuple_delimiter = true;

        /// Data for current substream.
        SubstreamData data;

        /// Creator of subcolumn for current substream.
        SubcolumnCreatorPtr creator = nullptr;

        /// Flag, that may help to traverse substream paths.
        mutable bool visited = false;

        Substream(Type type_) : type(type_) {}

        String toString() const;
    };

    struct SubstreamPath : public std::vector<Substream>
    {
        String toString() const;
    };

    /// Cache for common substreams of one type, but possible different its subcolumns.
    /// E.g. sizes of arrays of Nested data type.
    struct SubstreamCacheEntry
    {
        SubstreamCacheEntry(size_t rows_before_filter_, const ColumnPtr& column_):
            rows_before_filter(rows_before_filter_), column(column_) {}

        size_t rows_before_filter;
        ColumnPtr column;
    };
    using SubstreamsCache = std::unordered_map<String, SubstreamCacheEntry>;

    using StreamCallback = std::function<void(const SubstreamPath &)>;

    struct EnumerateStreamsSettings
    {
        SubstreamPath path;
        bool position_independent_encoding = true;
    };

    virtual void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const;

    /// Enumerate streams with default settings.
    void enumerateStreams(
        const StreamCallback & callback,
        const DataTypePtr & type = nullptr,
        const ColumnPtr & column = nullptr) const;

    void enumerateStreams(
        const StreamCallback & callback,
        const SubstreamPath & path,
        const DataTypePtr & type = nullptr,
        const ColumnPtr & column = nullptr) const;

    using OutputStreamGetter = std::function<WriteBuffer*(const SubstreamPath &)>;
    using InputStreamGetter = std::function<ReadBuffer*(const SubstreamPath &)>;
    using CompressedIndexGetter = std::function<std::shared_ptr<CompressedDataIndex>(const SubstreamPath &)>;

    struct SerializeBinaryBulkState
    {
        virtual ~SerializeBinaryBulkState() = default;
    };

    struct DeserializeBinaryBulkState
    {
        virtual ~DeserializeBinaryBulkState() = default;
    };

    using SerializeBinaryBulkStatePtr = std::shared_ptr<SerializeBinaryBulkState>;
    using DeserializeBinaryBulkStatePtr = std::shared_ptr<DeserializeBinaryBulkState>;

    struct SerializeBinaryBulkSettings
    {
        OutputStreamGetter getter;
        SubstreamPath path;

        size_t low_cardinality_max_dictionary_size = 0;
        bool low_cardinality_use_single_dictionary_for_part = true;

        bool position_independent_encoding = true;
    };

    struct DeserializeBinaryBulkSettings
    {
        InputStreamGetter getter;
        /// Some serialization method may need benefit from compressed index file
        /// it will utilize compressed data index to accelerate skip and seek
        CompressedIndexGetter compressed_idx_getter;
        SubstreamPath path;

        /// True if continue reading from previous positions in file. False if made fseek to the start of new granule.
        bool continuous_reading = true;

        bool position_independent_encoding = true;

        bool native_format = false;

        /// If not zero, may be used to avoid reallocations while reading column of String type.
        double avg_value_size_hint = 0;

        bool zero_copy_read_from_cache = false;

        /// If filter is not nullptr, serialization will filter readed data based on filter
        /// 0 in filter means skip corresponding row
        const UInt8* filter = nullptr;
    };

    /// Call before serializeBinaryBulkWithMultipleStreams chain to write something before first mark.
    /// Column may be used only to retrieve the structure.
    virtual void serializeBinaryBulkStatePrefix(
        const IColumn & /*column*/,
        SerializeBinaryBulkSettings & /*settings*/,
        SerializeBinaryBulkStatePtr & /*state*/) const {}

    /// Call after serializeBinaryBulkWithMultipleStreams chain to finish serialization.
    virtual void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & /*settings*/,
        SerializeBinaryBulkStatePtr & /*state*/) const {}

    /// Call before before deserializeBinaryBulkWithMultipleStreams chain to get DeserializeBinaryBulkStatePtr.
    virtual void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & /*settings*/,
        DeserializeBinaryBulkStatePtr & /*state*/) const {}

    /** 'offset' and 'limit' are used to specify range.
      * limit = 0 - means no limit.
      * offset must be not greater than size of column.
      * offset + limit could be greater than size of column
      *  - in that case, column is serialized till the end.
      */
    virtual void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

    /// Read no more than limit values and append them into column. Return processed rows
    virtual size_t deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const;

    /** Override these methods for data types that require just single stream (most of data types).
      */
    virtual void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const;
    virtual size_t deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit,
        double avg_value_size_hint, bool zero_copy_cache_read, const UInt8* filter) const;

    /** Serialization/deserialization of individual values.
      *
      * These are helper methods for implementation of various formats to input/output for user (like CSV, JSON, etc.).
      * There is no one-to-one correspondence between formats and these methods.
      * For example, TabSeparated and Pretty formats could use same helper method serializeTextEscaped.
      *
      * For complex data types (like arrays) binary serde for individual values may differ from bulk serde.
      * For example, if you serialize single array, it will be represented as its size and elements in single contiguous stream,
      *  but if you bulk serialize column with arrays, then sizes and elements will be written to separate streams.
      */

    /// There is two variants for binary serde. First variant work with Field.
    virtual void serializeBinary(const Field & field, WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(Field & field, ReadBuffer & istr) const = 0;

    /// Other variants takes a column, to avoid creating temporary Field object.
    /// Column must be non-constant.

    /// Serialize one value of a column at specified row number.
    virtual void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const = 0;
    /// Deserialize one value and insert into a column.
    /// If method will throw an exception, then column will be in same state as before call to method.
    virtual void deserializeBinary(IColumn & column, ReadBuffer & istr) const = 0;

    /** Text serialization with escaping but without quoting.
      */
    virtual void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    virtual void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization as a literal that may be inserted into a query.
      */
    virtual void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    virtual void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization for the CSV format.
      */
    virtual void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization for displaying on a terminal or saving into a text file, and the like.
      * Without escaping or quoting.
      */
    virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    /** Text deserialization in case when buffer contains only one value, without any escaping and delimiters.
      */
    virtual void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization intended for using in JSON format.
      */
    virtual void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization intended for memory comparasion.
      */
    /// A mem-comparable encoding guarantees that we can compare encoded value using memcmp without decoding
    virtual bool supportMemComparableEncoding() const { return false; }
    /// Serialize one value of a column into mem-comparable format.
    /// Throws exception when supportMemComparableEncoding() == false
    virtual void serializeMemComparable(const IColumn & column, size_t row_num, WriteBuffer & ostr) const;
    /// Deserialize one value from mem-comparable format and insert into a column.
    /// Throws exception when supportMemComparableEncoding() == false
    virtual void deserializeMemComparable(IColumn & column, ReadBuffer & istr) const;

    /** Text serialization for putting into the XML format.
      */
    virtual void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
    {
        serializeText(column, row_num, ostr, settings);
    }

    static String getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path);
    static String getFileNameForStream(const String & name_in_storage, const SubstreamPath & path);
    static String getSubcolumnNameForStream(const SubstreamPath & path);
    static String getSubcolumnNameForStream(const SubstreamPath & path, size_t prefix_len);

    static void addToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path,
        size_t rows_before_filter, ColumnPtr column);
    static std::optional<SubstreamCacheEntry> getFromSubstreamsCache(SubstreamsCache * cache,
        const SubstreamPath & path);

    static bool isSpecialCompressionAllowed(const SubstreamPath & path);

    static size_t getArrayLevel(const SubstreamPath & path);
    static bool hasSubcolumnForPath(const SubstreamPath & path, size_t prefix_len);
    static SubstreamData createFromPath(const SubstreamPath & path, size_t prefix_len);

    void setInSerializationNullable()
    {
        in_serialization_nullable = true;
    }

    static bool isSharedOffsetSubstream(const String & name_in_storage, const SubstreamPath & path);

protected:
    template <typename State, typename StatePtr>
    State * checkAndGetState(const StatePtr & state) const;
    [[noreturn]] void throwUnexpectedDataAfterParsedValue(IColumn & column, ReadBuffer & istr, const FormatSettings &, const String & type_name) const;

    /// set by SerializationNullable to indicate this is a
    /// nested serialization method inside SerializationNullable
    bool in_serialization_nullable = false;
};

using SerializationPtr = std::shared_ptr<const ISerialization>;
using Serializations = std::vector<SerializationPtr>;

template <typename State, typename StatePtr>
State * ISerialization::checkAndGetState(const StatePtr & state) const
{
    if (!state)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Got empty state for {}", demangle(typeid(*this).name()));

    auto * state_concrete = typeid_cast<State *>(state.get());
    if (!state_concrete)
    {
        auto & state_ref = *state;
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid State for {}. Expected: {}, got {}",
                demangle(typeid(*this).name()),
                demangle(typeid(State).name()),
                demangle(typeid(state_ref).name()));
    }

    return state_concrete;
}

}
