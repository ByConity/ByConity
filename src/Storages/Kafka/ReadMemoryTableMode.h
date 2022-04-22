#pragma once

#include <Core/Types.h>

namespace DB
{

enum class ReadMemoryTableMode
{
    ALL,
    PART,
    SKIP
};

const char * readModeToString(ReadMemoryTableMode mode);

ReadMemoryTableMode stringToReadMode(String mode);

}
