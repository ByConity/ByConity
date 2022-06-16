#pragma once

#include <common/types.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_map>

namespace DB
{

using Aliases = std::unordered_map<String, ASTPtr>;
using MultipleAliases = std::unordered_map<String, std::vector<String>>;

}
