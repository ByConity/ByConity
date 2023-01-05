#pragma once
#include <Columns/IColumn.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{
void createHiveBucketColumn(Block & block, const Block & bucket_columns, const Int64 & total_bucket_num, const ContextPtr & context);
Int64 getHiveBucket(DataTypePtr & type, ColumnPtr & column, String & name, const Int64 & total_bucket_num);
Int64 hashBytes(const String & str, int start, int length);
Int64 getBuckHashCode(DataTypePtr & type, ColumnPtr & column, String & name);
ColumnPtr createColumnWithHiveHash(Block & block, const Block & bucket_columns, const Int64 & total_bucket_num);
ASTs extractBucketColumnExpression(const ASTs & conditions, Names bucket_columns);

}
