#pragma once

#include <cstddef>
#include <Core/Defines.h>

namespace DB
{

/**
 * Mode of opening a file for write.
 */
enum class WriteMode
{
    Rewrite,
    Append
};

struct WriteSettings
{
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    WriteMode mode = WriteMode::Rewrite;
};

}
