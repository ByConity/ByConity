#include <DataTypes/MapHelpers.h>

#include <cstring>
#include <cctype>
#include <Common/escapeForFileName.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_IMPLICIT_COLUMN_NAME;
    extern const int INVALID_IMPLICIT_COLUMN_FILE_NAME;
}

std::string_view ExtractMapColumn::apply(std::string_view src)
{
    if (src.size() < strlen("__M__1.bin")) /// minimum example
        return {};
    if (src[0] != '_' || src[1] != '_') // start with __
        return {};

    size_t column_end = strlen("__"); /// start with __
    while (column_end + 1 < src.size() && !(src[column_end] == '_' && src[column_end + 1] == '_'))
        ++column_end;

    if (column_end + 1 == src.size()) /// not found
        return {};
    /// Just ignore chars after __

    return std::string_view(src.data() + 2, column_end - 2);
}

std::string_view ExtractMapKey::apply(std::string_view src, std::string_view * out_column)
{
    auto column = ExtractMapColumn::apply(src);
    if (column.size() == 0)
        return {};

    if (out_column)
        *out_column = column;

    const size_t column_part_size = column.size() + 4;
    size_t key_end = column_part_size;
    if (src[key_end] == '\'') /// begin with ' for Map(String, T)
    {
        key_end += 1;
        while (key_end + 1 < src.size() && !(src[key_end] == '\'' && src[key_end + 1] == '.')) /// end with '.bin or '.mrk
            ++key_end;

        if (key_end + 1 == src.size()) /// not found
            return {};
        /// Just ignore chars after '.

        return std::string_view(src.data() + column_part_size + 1, key_end - (column_part_size + 1));
    }
    else if (std::isdigit(src[key_end])) /// begin with digit for Map(Int*/Float*, T)
    {
        while (key_end + 1 < src.size() && !(src[key_end] == '.' && std::islower(src[key_end + 1]))) /// end with .bin or .mrk
            ++key_end;

        if (key_end + 1 == src.size()) /// not found
            return {};
        /// Just ignore chars after '.

        return std::string_view(src.data() + column_part_size, key_end - column_part_size);
    }
    else
        return {};
}

String genMapKeyFilePrefix(const String & column)
{
    return escapeForFileName("__" + column + "__'");
}

String genMapBaseFilePrefix(const String & column)
{
    return escapeForFileName("__" + column + "_base") + '.';
}

String getImplicitFileNameForMapKey(const String &mapCol, const String& keyName)
{
    // we don't escape keyName here as it will be wraped while adding stream based on this name
    return String("__") + mapCol + "__" + keyName;
}

String getBaseNameForMapCol(const String& mapCol)
{
    return String("__" + mapCol + "_base");
}

bool isMapImplicitKey(const String &mapCol)
{
    return startsWith(mapCol, "__") || endsWith(mapCol, ".key") || endsWith(mapCol, ".value");
}

bool isMapKV(const String &mapCol)
{
    return endsWith(mapCol, ".key") || endsWith(mapCol, ".value");
}

bool isMapImplicitKeyNotKV(const String &mapCol)
{
    return startsWith(mapCol, "__") ;
}

bool parseKeyFromImplicitFileName(const String& mapCol,
                                  const String& dataFileName,
                                  String & implicitKeyName)
{
    if (isImplicitDataFileNameOfSpecificMapCol(mapCol, dataFileName))
    {
        // Special case for array
        size_t pos = dataFileName.find(".size");
        if (pos != String::npos)
            return false;

        // Special case for null
        pos = dataFileName.find(".null");
        if (pos != String::npos)
            return false;

        // Special case for lc type
        pos = dataFileName.find(".dict");
        if (pos != String::npos)
            return false;

        size_t prefixLen = mapCol.length() + 4;
        size_t suffixLen = std::strlen(DATA_FILE_EXTENSION);
        String unescape_file_name = unescapeForFileName(dataFileName);
        //TBD: for some type it maybe quoted
        implicitKeyName = unescape_file_name.substr(prefixLen, unescape_file_name.length() - prefixLen - suffixLen);

        return true;
    }

    return false;
}


bool isImplicitDataFileNameOfSpecificMapCol(const String& mapCol, const String& dataFileName)
{
    // check prefix and suffix of the implicit col file name
   String unescape_file_name = unescapeForFileName(dataFileName);
   if (startsWith(unescape_file_name, String("__") + mapCol + "__") && endsWith(unescape_file_name, DATA_FILE_EXTENSION))
   {
       // check suffix of implicit col name
       // In the case that there are two map col: a (Map<Int64, Int64>) and a__1 (Map<Int64, Int64>)
       // if col a__1 has a key 1, the implicit col will be __a__1__1, when we query for col a, __a__1__1 is invalid.
       String suffix = unescape_file_name.substr(mapCol.length() + 4);
       return suffix.find("__") == String::npos || startsWith(suffix, "\'");
   }
   return false;
}

String parseColNameFromImplicitName(const String& implicitColumnName)
{
    if (!startsWith(implicitColumnName, "__"))
    {
        throw Exception("Invalid implicit name: " + implicitColumnName, ErrorCodes::INVALID_IMPLICIT_COLUMN_NAME);
    }

    /// Due to map column name does not contain "__" and end with '_', so we can find second "__" directly.
    auto location = implicitColumnName.find("__", 2);
    if (location == String::npos)
    {
        throw Exception("Invalid implicit name: " + implicitColumnName, ErrorCodes::INVALID_IMPLICIT_COLUMN_NAME);
    }
    return implicitColumnName.substr(2, location - 2);
}

/// Input parameter 'implicitColFileName' is from checksum, which has escaped by method escapeForFileName. For detail, please refer to IDataType::getFileNameForStream.
/// In compact map version, all implicit columns are stored in the same file with different offsets, this method is extract the target file name which consist of map column name and extension.
String getColFileNameFromImplicitColFileName(const String & implicitColFileName)
{
    String col_name = parseColNameFromImplicitName(implicitColFileName);
    auto extension_loc = implicitColFileName.find('.'); /// only extension contain dot, other dots in column name are escaped.
    if (extension_loc == String::npos)
    {
        throw Exception("Invalid file name of implicit column when dumping part: " + implicitColFileName, ErrorCodes::INVALID_IMPLICIT_COLUMN_FILE_NAME);
    }
    return col_name + implicitColFileName.substr(extension_loc);
}


String parseKeyFromImplicitMap(const String & map_column, const String & implicit_column)
{
    String res;
    if (startsWith(implicit_column, String("__") + map_column + "__"))
    {
        size_t prefix_len = map_column.length() + 4;
        String tmp_name = implicit_column.substr(prefix_len);
        const char * pos = tmp_name.data();
        const char * end = pos + tmp_name.size();
        while (pos != end)
        {
            unsigned char c = *pos;
            if (isWordCharASCII(c))
                res += c;
            ++pos;
        }
    }
    return res;
}

// Get map column name from its implicit key column.
// i.e. __col__key.... --> col
bool parseMapFromImplName(const String & implCol, String & mapCol)
{
    // assume implCol start with "__" and has been checked in its caller
    size_t i = 2;
    size_t collen = implCol.size() - 1;
    for(; i < collen; i++)
    {
        if (implCol[i] == '_' &&
            implCol[i+1] == '_')
        {
            return true;
        }
        mapCol.push_back(implCol[i]);
    } 

    return false;
}

}
