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

#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeHelper.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/MapHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Protos/plan_node_utils.pb.h>
#include <sparsehash/dense_hash_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}

NameAndTypePair::NameAndTypePair(
    const String & name_in_storage_, const String & subcolumn_name_,
    const DataTypePtr & type_in_storage_, const DataTypePtr & subcolumn_type_)
    : name(name_in_storage_ + (subcolumn_name_.empty() ? "" : "." + subcolumn_name_))
    , type(subcolumn_type_)
    , type_in_storage(type_in_storage_)
    , subcolumn_delimiter_position(subcolumn_name_.empty() ? std::nullopt : std::make_optional(name_in_storage_.size()))
{
}

String NameAndTypePair::getNameInStorage() const
{
    if (!subcolumn_delimiter_position)
        return name;

    return name.substr(0, *subcolumn_delimiter_position);
}

String NameAndTypePair::getSubcolumnName() const
{
    if (!subcolumn_delimiter_position)
        return "";

    return name.substr(*subcolumn_delimiter_position + 1, name.size() - *subcolumn_delimiter_position);
}

void NameAndTypePair::serialize(WriteBuffer & buf) const
{
    writeBinary(name, buf);
    serializeDataType(type, buf);
    serializeDataType(type_in_storage, buf);
    if (subcolumn_delimiter_position)
    {
        writeBinary(true, buf);
        writeBinary(subcolumn_delimiter_position.value(), buf);
    }
    else
        writeBinary(false, buf);
}

void NameAndTypePair::deserialize(ReadBuffer & buf)
{
    readBinary(name, buf);
    type = deserializeDataType(buf);
    type_in_storage = deserializeDataType(buf);

    bool has_size;
    readBinary(has_size, buf);
    if (has_size)
    {
        size_t subcolumn_tmp;
        readBinary(subcolumn_tmp, buf);
        subcolumn_delimiter_position = subcolumn_tmp;
    }
}

void NameAndTypePair::toProto(Protos::NameAndTypePair & proto) const
{
    proto.set_name(name);
    serializeDataTypeToProto(type, *proto.mutable_type());
    serializeDataTypeToProto(type_in_storage, *proto.mutable_type_in_storage());
    if (subcolumn_delimiter_position.has_value())
        proto.set_subcolumn_delimiter_position(subcolumn_delimiter_position.value());
}

void NameAndTypePair::fillFromProto(const Protos::NameAndTypePair & proto)
{
    name = proto.name();
    type = deserializeDataTypeFromProto(proto.type());
    type_in_storage = deserializeDataTypeFromProto(proto.type_in_storage());
    if (proto.has_subcolumn_delimiter_position())
        subcolumn_delimiter_position = proto.subcolumn_delimiter_position();
}

void NamesAndTypesList::readText(ReadBuffer & buf)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    assertString("columns format version: 1\n", buf);
    size_t count;
    DB::readText(count, buf);
    assertString(" columns:\n", buf);
    resize(count);

    for (NameAndTypePair & it : *this)
    {
        readBackQuotedStringWithSQLStyle(it.name, buf);
        assertChar(' ', buf);
        String type_name;
        readString(type_name, buf);
        it.type = data_type_factory.get(type_name);

        if (*buf.position() == '\n')
        {
            assertChar('\n', buf);
            continue;
        }

        assertChar('\t', buf);
        // optional settings
        String options;
        readString(options, buf);

        while (!options.empty())
        {
            if (options == "KV")
            {
                it.type->setFlags(TYPE_MAP_KV_STORE_FLAG);
            }
            else if (options == "BYTE")
            {
                const_cast<IDataType *>(it.type.get())->setFlags(TYPE_MAP_BYTE_STORE_FLAG);
            }
            else if(options == "COMPRESSION")
            {
                it.type->setFlags(TYPE_COMPRESSION_FLAG);
            }
            else if (options == "BitEngineEncode")
            {
                const_cast<IDataType *>(it.type.get())->setFlags(TYPE_BITENGINE_ENCODE_FLAG);
            }
            else if (options == "BLOOM")
            {
                it.type->setFlags(TYPE_BLOOM_FLAG);
            }
            else if (options == "BitmapIndex")
            {
                it.type->setFlags(TYPE_BITMAP_INDEX_FLAG);
            }
            else if (options == "SegmentBitmapIndex")
            {
                it.type->setFlags(TYPE_SEGMENT_BITMAP_INDEX_FLAG);
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown flag {}", options);
            }

            if (*buf.position() == '\n')
                break;

            assertChar('\t', buf);
            readString(options, buf);
        }

        assertChar('\n', buf);
    }
}

void NamesAndTypesList::writeText(WriteBuffer & buf) const
{
    writeString("columns format version: 1\n", buf);
    DB::writeText(size(), buf);
    writeString(" columns:\n", buf);
    for (const auto & it : *this)
    {
        writeBackQuotedString(it.name, buf);
        writeChar(' ', buf);
        writeString(it.type->getName(), buf);

        UInt16 flag = it.type->getFlags();

        while (flag)
        {
            if (flag & TYPE_COMPRESSION_FLAG)
            {
                writeChar('\t', buf);
                writeString("COMPRESSION", buf);
                flag ^= TYPE_COMPRESSION_FLAG;
            }
            else if (flag & TYPE_BLOOM_FLAG)
            {
                writeChar('\t', buf);
                writeString("BLOOM", buf);
                flag ^= TYPE_BLOOM_FLAG;
            }
            else if (flag & TYPE_BITMAP_INDEX_FLAG)
            {
                writeChar('\t', buf);
                writeString("BitmapIndex", buf);
                flag ^= TYPE_BITMAP_INDEX_FLAG;
            }
            else if (flag & TYPE_SEGMENT_BITMAP_INDEX_FLAG)
            {
                writeChar('\t', buf);
                writeString("SegmentBitmapIndex", buf);
                flag ^= TYPE_SEGMENT_BITMAP_INDEX_FLAG;
            }
            else if (flag & TYPE_MAP_KV_STORE_FLAG)
            {
                writeChar('\t', buf);
                writeString("KV", buf);
                flag ^= TYPE_MAP_KV_STORE_FLAG;
            }
            else if (flag & TYPE_MAP_BYTE_STORE_FLAG)
            {
                writeChar('\t', buf);
                writeString("BYTE", buf);
                flag ^= TYPE_MAP_BYTE_STORE_FLAG;
            }
            else if (flag & TYPE_BITENGINE_ENCODE_FLAG)
            {
                writeChar('\t', buf);
                writeString("BitEngineEncode", buf);
                flag ^= TYPE_BITENGINE_ENCODE_FLAG;
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown flag {}", flag);
        }

        writeChar('\n', buf);
    }
}

String NamesAndTypesList::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

NamesAndTypesList NamesAndTypesList::parse(const String & s)
{
    ReadBufferFromString in(s);
    NamesAndTypesList res;
    res.readText(in);
    assertEOF(in);
    return res;
}

bool NamesAndTypesList::isSubsetOf(const NamesAndTypesList & rhs) const
{
    NamesAndTypes vector(rhs.begin(), rhs.end());
    vector.insert(vector.end(), begin(), end());
    std::sort(vector.begin(), vector.end());
    return std::unique(vector.begin(), vector.end()) == vector.begin() + rhs.size();
}

size_t NamesAndTypesList::sizeOfDifference(const NamesAndTypesList & rhs) const
{
    NamesAndTypes vector(rhs.begin(), rhs.end());
    vector.insert(vector.end(), begin(), end());
    std::sort(vector.begin(), vector.end());
    return (std::unique(vector.begin(), vector.end()) - vector.begin()) * 2 - size() - rhs.size();
}

bool NamesAndTypesList::isCompatableWithKeyColumns(const NamesAndTypesList & rhs, const Names & keys_columns)
{
    if (keys_columns.size() > 1)
    {
        NameSet keys(keys_columns.begin(), keys_columns.end());
        NamesAndTypes k1, k2;
        for (const NameAndTypePair & column : *this)
        {
            if (keys.count(column.name))
                k1.push_back(column);
        }
        for (const NameAndTypePair & column : rhs)
        {
            if (keys.count(column.name))
                k2.push_back(column);
        }

        if (k1 != k2)
            return false;
    }

    return isSubsetOf(rhs) || rhs.isSubsetOf(*this);
}

void NamesAndTypesList::getDifference(const NamesAndTypesList & rhs, NamesAndTypesList & deleted, NamesAndTypesList & added) const
{
    NamesAndTypes lhs_vector(begin(), end());
    std::sort(lhs_vector.begin(), lhs_vector.end());
    NamesAndTypes rhs_vector(rhs.begin(), rhs.end());
    std::sort(rhs_vector.begin(), rhs_vector.end());

    std::set_difference(lhs_vector.begin(), lhs_vector.end(), rhs_vector.begin(), rhs_vector.end(),
        std::back_inserter(deleted));
    std::set_difference(rhs_vector.begin(), rhs_vector.end(), lhs_vector.begin(), lhs_vector.end(),
        std::back_inserter(added));
}

Names NamesAndTypesList::getNames() const
{
    Names res;
    res.reserve(size());
    for (const NameAndTypePair & column : *this)
        res.push_back(column.name);
    return res;
}

DataTypes NamesAndTypesList::getTypes() const
{
    DataTypes res;
    res.reserve(size());
    for (const NameAndTypePair & column : *this)
        res.push_back(column.type);
    return res;
}

NamesAndTypesList NamesAndTypesList::filter(const NameSet & names) const
{
    NamesAndTypesList res;
    for (const NameAndTypePair & column : *this)
    {
        if (names.count(column.name))
            res.push_back(column);
    }
    return res;
}

NamesAndTypesList NamesAndTypesList::filter(const Names & names) const
{
    return filter(NameSet(names.begin(), names.end()));
}

NamesAndTypesList NamesAndTypesList::addTypes(const Names & names, BitEngineReadType bitengine_read_type) const
{
    /// NOTE: It's better to make a map in `IStorage` than to create it here every time again.
#if !defined(ARCADIA_BUILD)
    google::dense_hash_map<StringRef, const DataTypePtr *, StringRefHash> types;
#else
    google::sparsehash::dense_hash_map<StringRef, const DataTypePtr *, StringRefHash> types;
#endif
    types.set_empty_key(StringRef());

    for (const auto & column : *this)
        types[column.name] = &column.type;

    NamesAndTypesList res;
    for (const String & name : names)
    {
        if (isMapImplicitKey(name))
        {
            String map_name = parseMapNameFromImplicitColName(name);
            auto it = types.find(map_name);
            if (it == types.end())
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "No column {} when handling implicit column {}", map_name, name);
            if (!(*it->second)->isByteMap())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Data type {} of column {} is not ByteMap as expected when handling implicit column {}.",
                    (*it->second)->getName(),
                    map_name,
                    name);
            res.emplace_back(name, typeid_cast<const DataTypeMap &>(*(*it->second)).getValueTypeForImplicitColumn());
        }
        else
        {
            auto it = types.find(name);
            if (it == types.end())
            {
                // if (endsWith(name, COMPRESSION_COLUMN_EXTENSION))
                //     res.emplace_back(name, std::make_shared<DataTypeUInt16>());
                // else
                throw Exception("No column " + name, ErrorCodes::THERE_IS_NO_COLUMN);
            }

            if (isBitmap64(*it->second) && (*it->second)->isBitEngineEncode())
            { /// Type `BOTH` is used in BitEngineDictionaryManager::checkEncodedPart
                if (bitengine_read_type == BitEngineReadType::BOTH && res.contains(name))
                    res.emplace_back(name + BITENGINE_COLUMN_EXTENSION, *it->second);
                else if (bitengine_read_type == BitEngineReadType::ONLY_ENCODE)
                { // Type `ONLY_ENCODE` used in BitEngine parts merge
                    res.emplace_back(name + BITENGINE_COLUMN_EXTENSION, *it->second);
                }
                else // Default type `ONLY_SOURCE` is used in encoding and select
                    res.emplace_back(name, *it->second);
            }
            else
            {
                res.emplace_back(name, *it->second);
            }
        }
    }

    return res;
}

bool NamesAndTypesList::contains(const String & name) const
{
    for (const NameAndTypePair & column : *this)
    {
        if (column.name == name)
            return true;
    }
    return false;
}

std::optional<NameAndTypePair> NamesAndTypesList::tryGetByName(const std::string & name) const
{
    for (const NameAndTypePair & column : *this)
    {
        if (column.name == name)
            return column;
    }
    return {};
}

size_t NamesAndTypesList::getPosByName(const std::string &name) const noexcept
{
    size_t pos = 0;
    for (const NameAndTypePair & column : *this)
    {
        if (column.name == name)
            break;
        ++pos;
    }
    return pos;
}


}
