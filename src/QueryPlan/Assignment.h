#pragma once

#include <utility>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Common/LinkedHashMap.h>

namespace DB
{
using Assignment = std::pair<String, ConstASTPtr>;
// using Assignments = std::vector<Assignment>;
using Assignments = LinkedHashMap<String, ConstASTPtr>;

}
