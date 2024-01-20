#pragma once

#include <common/types.h>
#include <string_view>

namespace DB
{

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
String likePatternToRegexpWithEscape(std::string_view pattern, char escape_char);

String likePatternToRegexp(std::string_view pattern);

}
