#pragma once

#include <string>
#include <common/StringRef.h>

namespace DB
{

StringRef parentPath(StringRef path);

StringRef getBaseName(StringRef path);

}
