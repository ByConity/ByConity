#pragma once

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

struct ColumnWithTypeAndName;
class TableJoin;
class IColumn;

using ColumnRawPtrs = std::vector<const IColumn *>;
using ColumnPtrMap = std::unordered_map<String, ColumnPtr>;
using ColumnRawPtrMap = std::unordered_map<String, const IColumn *>;
using UInt8ColumnDataPtr = const ColumnUInt8::Container *;

namespace JoinCommon
{

ColumnPtr materializeColumn(const Block & block, const String & name);

Blocks scatterBlockByHash(const Strings & key_columns_names, const Block & block, size_t num_shards);

}

}
