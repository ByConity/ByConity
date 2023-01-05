#include <QueryPlan/SymbolAllocator.h>

#include <Poco/NumberParser.h>

namespace DB
{

String SymbolAllocator::newSymbol(const ASTPtr & expression)
{
    return newSymbol("expr#" + expression->getColumnName());
}

String SymbolAllocator::newSymbol(String name_hint)
{
    // extract name prefix if name_hint is from a symbol
    {
        auto index = name_hint.rfind('_');
        int ignored;

        if (index != String::npos && Poco::NumberParser::tryParse(name_hint.substr(index + 1), ignored))
        {
            name_hint = name_hint.substr(0, index);
        }
    }

    if (next_ids.find(name_hint) == next_ids.end())
        next_ids[name_hint] = 1;

    String name = name_hint;

    while (symbols.find(name) != symbols.end())
    {
        name = name_hint + '_' + std::to_string(next_ids[name_hint]++);
    }

    symbols.insert(name);
    return name;
}

}
