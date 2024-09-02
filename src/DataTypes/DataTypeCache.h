#pragma once

#include <Parsers/IAST.h>
#include <Core/Types.h>
#include <unordered_map>

namespace DB
{

class DataTypeCache
{
private:
    std::unordered_map<String, ASTPtr> cells;

public:
    DataTypeCache();

    ASTPtr tryGet(const String & key) const;
};

using DataTypeCachePtr = std::shared_ptr<DataTypeCache>;

}
