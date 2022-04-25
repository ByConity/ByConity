#pragma once
#include <string_view>
#include <common/types.h>

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

class IDataType;

// Convert key name to visitor string
// i.e. 'key' -> key
String convertKeyNameToVisitorString(const IDataType * key_type, String key_name);

/********************************************************************************************************************************
 * Attention: In all the following methods, whether input or output parameters, file name is escaped, and others are not escaped.
 * What's more, all key names have been quoted by method DB::FieldVisitorToString().
 ********************************************************************************************************************************/

const String & getMapSeparator();

// Throw exception if map separator contains any character which isn't belong to word char ASCII.
void checkAndSetMapSeparator(const String & map_separator_);

// Get map key file prefix name which has been escaped.
// i.e. col. -> __col%2E__
String genMapKeyFilePrefix(const String & column);

// Get map base file prefix name which has been escaped.
// i.e. col. -> __col%2E_base.
String genMapBaseFilePrefix(const String & column);

// Get implicit map column name based on map column name and key name
// i.e. col, 'key' -> __col__'key'
String getImplicitColNameForMapKey(const String & map_col, const String & key_name);

// Get map base name
// i.e. col -> __col_base
String getBaseNameForMapCol(const String & map_col);

// Wrap map name with separator
// i.e. col -> __col__
String getMapKeyPrefix(const String & column);

// Get map name from its implicit file name.
// i.e. __col__%27key%27.bin --> col
String parseMapNameFromImplicitFileName(const String & implicit_file_name);

// Get key name from its implicit file name.
// i.e. __col__%27key%27.bin, col --> 'key'
String parseKeyNameFromImplicitFileName(const String & implicit_file_name, const String & map_col);

// Get map name from its implicit column name.
// i.e. __col__'key' --> col
String parseMapNameFromImplicitColName(const String & implicit_col);

// Get key name from its implicit column name.
// i.e. __col__'key' --> 'key'
String parseKeyNameFromImplicitColName(const String & implicit_col, const String & map_col);

// Input parameter 'implicit_file_name' is from checksum, which has escaped by method escapeForFileName. For detail, please refer to ISerialization::getFileNameForStream.
// In compact map version, all implicit columns are stored in the same file with different offsets, this method is extract the target file name which consist of map column name and extension.
// i.e. __col1__%27key%27.bin --> col.bin, __col2__%27key%27.null.bin --> col2.null.bin
String getMapFileNameFromImplicitFileName(const String & implicit_file_name);

bool isMapBaseFile(const String & file_name);

bool isMapImplicitKey(const String & map_col);

bool isMapImplicitKeyNotKV(const String & map_col);

bool isMapKV(const String & map_col);

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

/// For compact map type, check if it's the compacted file of target map column.
// i.e. col.null.bin, co -> false(not belong to target map column)
// i.e. col.bin, col -> true
bool isMapCompactFileNameOfSpecialMapName(const String file_name, const String map_col);
}
