#include <QueryPlan/planning_common.h>

namespace DB
{

void putIdentities(const NamesAndTypes & columns, Assignments & assignments, NameToType & types)
{
    for (const auto & col: columns)
    {
        assignments.emplace_back(col.name, toSymbolRef(col.name));
        types[col.name] = col.type;
    }
}

FieldSymbolInfos mapSymbols(const FieldSymbolInfos & field_symbol_infos, const std::unordered_map<String, String> & old_to_new)
{
    FieldSymbolInfos result;

    auto find_new_symbol = [&](const auto & old)
    {
        if (auto it = old_to_new.find(old); it != old_to_new.end())
            return it->second;

        throw Exception("Symbol not found", ErrorCodes::LOGICAL_ERROR);
    };

    for (const auto & old_info : field_symbol_infos)
    {
        const auto & sub_column_symbols = old_info.sub_column_symbols;
        FieldSymbolInfo::SubColumnToSymbol new_sub_column_symbols;

        for (const auto & sub_col_item: sub_column_symbols)
            new_sub_column_symbols.emplace(sub_col_item.first, find_new_symbol(sub_col_item.second));

        result.emplace_back(find_new_symbol(old_info.primary_symbol), new_sub_column_symbols);
    }

    return result;
}

}
