#include "ShardPartition.h"
#include "ShardPartitionPiece.h"

namespace DB {
ShardPartition::ShardPartition(TaskShard & parent, String name_quoted_, size_t number_of_splits)
    : task_shard(parent), name(std::move(name_quoted_))
{
    pieces.reserve(number_of_splits);
}
}
