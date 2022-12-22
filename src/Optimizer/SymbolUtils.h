#pragma once
#include <Core/Field.h>
#include <set>

namespace DB
{
class SymbolUtils
{
public:
    static bool contains(std::vector<String> & symbols, String symbol);
    static bool containsAll(std::set<String> & left_symbols, std::set<String> & right_symbols);
    static bool containsAll(std::vector<String> & left_symbols, std::set<String> & right_symbols);

};

}
