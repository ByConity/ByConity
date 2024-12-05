/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <string_view>
#include <common/types.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/IDataType.h>

namespace DB
{
struct ExtractMapColumn
{
    static std::string_view apply(std::string_view src);
};

struct ExtractMapKey
{
    inline static std::string_view apply(std::string_view src) { return apply(src, nullptr); } /// only extact key
    static std::string_view apply(std::string_view src, std::string_view * column);
};

// Keywords compatible with community map and ByteMap KV
const String MAP_KV_RESERVED_KEYS[4] = {".keys", ".key", ".values", ".value"};

// Try convert key to field based on data type, return Null if failed
Field tryConvertToMapKeyField(const DataTypePtr & key_type, const String & key_name);

// Convert key to field based on data type
Field convertToMapKeyField(const DataTypePtr & key_type, const String & key_name);

// Convert key to field based on data type and then apply FieldVisitorToString
String convertToMapKeyString(const DataTypePtr & key_type, const String & key_name);

/********************************************************************************************************************************
 * Attention: In all the following methods, whether input or output parameters, file name is escaped, and others are not escaped.
 * What's more, all key names have been quoted by method DB::FieldVisitorToString().
 ********************************************************************************************************************************/

const String & getMapSeparator();

// Throw exception if map separator contains any character which isn't belong to word char ASCII.
void checkAndSetMapSeparator(const String & map_separator_);

// Get map key file prefix name which has been escaped.
// e.g. col. -> __col%2E__
String genMapKeyFilePrefix(const String & column);

// Get map base file prefix name which has been escaped.
// e.g. col. -> __col%2E_base.
String genMapBaseFilePrefix(const String & column);

// Get implicit map column name based on map column name and key name
// e.g. col, 'key' -> __col__'key'
String getImplicitColNameForMapKey(const String & map_col, const String & key_name);

// Get implicit map file name prefix based on map column name and key name
// e.g. col, 'key' -> __col__%27key%27
String getImplicitFileNamePrefixForMapKey(const String & map_col, const String & key_name);

// Get map base name
// e.g. col -> __col_base
String getBaseNameForMapCol(const String & map_col);

// Wrap map name with separator
// e.g. col -> __col__
String getMapKeyPrefix(const String & column);

// Get map name from its implicit file name.
// e.g. __col__%27key%27.bin --> col
String parseMapNameFromImplicitFileName(const String & implicit_file_name);

// Get key name from its implicit file name.
// e.g. __col__%27key%27.bin, col --> 'key'
String parseKeyNameFromImplicitFileName(const String & implicit_file_name, const String & map_col);

// Get implicit column name from its implicit file name.
// e.g. __col__%27key%27.bin, col --> __col__'key'
String parseImplicitColumnFromImplicitFileName(const String & implicit_file_name, const String & map_col);

// Get map name from its implicit column name.
// e.g. __col__'key' --> col
String parseMapNameFromImplicitColName(const String & implicit_col);

// Get key name from its implicit column name.
// e.g. __col__'key' --> 'key'
String parseKeyNameFromImplicitColName(const String & implicit_col, const String & map_col);

// Input parameter 'implicit_file_name' is from checksum, which has escaped by method escapeForFileName. For detail, please refer to ISerialization::getFileNameForStream.
// In compact map version, all implicit columns are stored in the same file with different offsets, this method is extract the target file name which consist of map column name and extension.
// e.g. __col1__%27key%27.bin --> col.bin, __col2__%27key%27.null.bin --> col2.null.bin
String getMapFileNameFromImplicitFileName(const String & implicit_file_name);

// Check if name contains map reserved keys, and return a name without the suffix
std::pair<bool, String> mayBeMapKVReservedKeys(const String & name);

bool isMapBaseFile(const String & file_name);

bool isMapImplicitKey(const String & map_col);

bool isMapImplicitKeyOfSpecialMapName(const String & implicit_col, const String & map_col);

// Community map data files ends with "%2Ekeys" and "%2Evalues" while ByteKV map data files ends with "%2Ekey" and "%2Evalue"
// After merging these two types, the merged types end with "%2Ekey" and "%2Evalue", so additional data compatible with the community version of map is required.
// Therefore, here only consider the case that convert "%2Ekey" -> "%2Ekeys" and "%2Evalue" -> "%2Evalues"
bool tryConvertToValidKVStreamName(String & stream_name, std::function<bool(String & stream_name)> check_validity);

// Check if the implicit file belongs to the target map column excluding base file and if it's data file whose suffix is .bin(exclude .null.bin or .size.bin)
// i.e. __col__%271%27.bin, co -> false(not belong to target map column)
// i.e. __col__%271%27.null.bin, col -> false(not data file)
// i.e. __col__%271%27.bin, col -> true
bool isMapImplicitDataFileNameNotBaseOfSpecialMapName(const String file_name, const String map_col);

// Check if the implicit file belongs to the target map column, including base file
// i.e. __col_base.bin, co -> false(not belong to target map column)
// i.e. __col_base.bin, col -> true
// i.e. __col__%271%27.mrk2, col -> true
bool isMapImplicitFileNameOfSpecialMapName(const String file_name, const String map_col);

// Check if the implicit file belongs to the target map column, including base file and if it's data file whose suffix is .bin(exclude .null.bin or .size.bin)
// i.e. __col_base.bin, co -> false(not belong to target map column)
// i.e. __col_base.bin, col -> true
// i.e. __col__%271%27.null.bin, col -> false(not data file)
// i.e. __col__%271%27.bin, col -> true
bool isMapImplicitDataFileNameOfSpecialMapName(const String & file_name, const String map_col);

/// Get range of files from ordered files (e.g. std::map) with a hacking solution.
template <class M>
std::pair<typename M::const_iterator, typename M::const_iterator>
getFileRangeFromOrderedFilesByPrefix(const String & prefix, const M & m)
{
    static_assert(std::is_same_v<String, typename M::key_type>);

    constexpr auto char_max = std::numeric_limits<String::value_type>::max();
    auto beg = m.lower_bound(prefix);
    /// Adding char_max to the end to speed up finding upper bound
    auto end = m.upper_bound(prefix + char_max);
    /// Maybe some files with prefix `prefix + char_max` exists
    while (end != m.end() && startsWith(end->first, prefix))
        end++;
    return {beg, end};
}

/// Get range of implicit column files from ordered files (e.g. std::map) with a hacking solution
template <class M>
std::pair<typename M::const_iterator, typename M::const_iterator>
getMapColumnRangeFromOrderedFiles(const String & map_column, const M & m)
{
    static_assert(std::is_same_v<String, typename M::key_type>);

    String map_prefix = escapeForFileName(getMapKeyPrefix(map_column));
    return getFileRangeFromOrderedFilesByPrefix(map_prefix, m);
}

}
