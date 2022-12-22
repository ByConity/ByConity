#pragma once
#include <string_view>
#include <Core/Types.h>
#include <common/StringRef.h>

namespace DB::Statistics
{
String base64Decode(std::string_view encoded);
String base64Encode(std::string_view decoded);

// inline String base64Decode(const StringRef& encoded) {
//     return base64Decode((encoded));
// }
// inline String base64Encode(const StringRef& decoded) {
//     return base64Encode(static_cast<std::string_view>(decoded));
// }
}

namespace DB
{
using Statistics::base64Decode;
using Statistics::base64Encode;
}
