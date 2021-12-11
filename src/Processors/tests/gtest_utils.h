#pragma once

#include <cstddef>
#include <Columns/IColumn.h>
#include <Processors/Chunk.h>
#include <Columns/ColumnsNumber.h>
#include <common/types.h>

namespace UnitTest
{

extern DB::Chunk createUInt8Chunk(size_t row_num, size_t column_num, UInt8 value);

}
