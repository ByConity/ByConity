#include <DataTypes/DataTypeCache.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>

namespace DB
{

DataTypeCache::DataTypeCache()
{
    /// only cover some common types.
    /// can be extended in need.
    std::vector<String> type_names = {
        "UInt8", "UInt16", "UInt32", "UInt64", "UInt128",
        "Int8", "Int16", "Int32", "Int64", "Int128",
        "Float32", "Float64",
        "Decimal32", "Decimal64",
        "Date", "DateTime", "DateTime64",
        "String",
        "Enum8", "Enum16"};
    std::vector<String> type_names_with_nullable;
    for (const auto & type_name : type_names)
    {
        type_names_with_nullable.emplace_back("Nullable(" + type_name + ")");
    }

    static constexpr size_t data_type_max_parse_depth = 200;

    ParserDataType parser;
    for (const auto & type_name : type_names)
    {
        cells.emplace(type_name, parseQuery(parser, type_name.data(), type_name.data() + type_name.size(), "data type", 0, data_type_max_parse_depth));
    }
    for (const auto & type_name : type_names_with_nullable)
    {
        cells.emplace(type_name, parseQuery(parser, type_name.data(), type_name.data() + type_name.size(), "data type", 0, data_type_max_parse_depth));
    }
}

ASTPtr DataTypeCache::tryGet(const String & key) const
{
    auto it = cells.find(key);
    if (it == cells.end())
        return nullptr;
    return it->second;
}
}
