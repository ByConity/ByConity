#pragma once

#include <cstddef>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <common/types.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace UnitTest
{
extern DB::Chunk createUInt8Chunk(size_t row_num, size_t column_num, UInt8 value);

extern DB::Block createUInt64Block(size_t row_num, size_t column_num, UInt8 value);

extern DB::ExecutableFunctionPtr createRepartitionFunction(DB::ContextPtr context, const DB::ColumnsWithTypeAndName & arguments);

}
