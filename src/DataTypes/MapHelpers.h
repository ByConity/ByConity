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

String genMapKeyFilePrefix(const String & column);

String genMapBaseFilePrefix(const String & column);

String getImplicitFileNameForMapKey(const String &mapCol, const String& keyName);

String getBaseNameForMapCol(const String& mapCol);

bool isMapImplicitKey(const String &mapCol);

bool isMapKV(const String &mapCol);

bool isMapImplicitKeyNotKV(const String &mapCol);

bool parseKeyFromImplicitFileName(const String& mapCol,
                                  const String& dataFileName,
                                  String & implicitKeyName);

bool isImplicitDataFileNameOfSpecificMapCol(const String& mapCol,
                                        const String& dataFileName);

String parseColNameFromImplicitName(const String& implicitColumnName);

String getColFileNameFromImplicitColFileName(const String & impliciteColFileName);

String parseKeyFromImplicitMap(const String & map_column, const String & implicit_column);

bool parseMapFromImplName(const String & implCol, String & mapCol);

}
