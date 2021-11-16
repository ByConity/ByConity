#pragma once

#include <ctime>

namespace DB
{

struct Thresholds
{
    time_t time;    /// The number of seconds from the insertion of the first row into the block.
    size_t rows;    /// The number of rows in the block.
    size_t bytes;   /// The number of (uncompressed) bytes in the block.
};

}
