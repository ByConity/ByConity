#pragma once

#include <Core/Types.h>
#include <Parsers/IAST.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace DB
{
class SymbolAllocator;
using SymbolAllocatorPtr = std::shared_ptr<SymbolAllocator>;

class SymbolAllocator
{
public:
    String newSymbol(String name_hint);
    String newSymbol(const ASTPtr & expression);

private:
    std::unordered_set<String> symbols;
    std::unordered_map<String, UInt32> next_ids;
};

}
