#include <Optimizer/SymbolUtils.h>

namespace DB
{
bool SymbolUtils::contains(std::vector<String> & symbols, String symbol)
{
    if (std::find(symbols.begin(), symbols.end(), symbol) != symbols.end())
    {
        return true;
    }
    return false;
}

bool SymbolUtils::containsAll(std::set<String> & left_symbols, std::set<String> & right_symbols)
{
    for (auto & symbol : right_symbols)
    {
        if (!left_symbols.contains(symbol))
        {
            return false;
        }
    }
    return true;
}

bool SymbolUtils::containsAll(std::vector<String> & left_symbols, std::set<String> & right_symbols)
{
    for (auto & symbol : right_symbols)
    {
        if (std::find(left_symbols.begin(), left_symbols.end(), symbol) == left_symbols.end())
        {
            return false;
        }
    }
    return true;
}

}
