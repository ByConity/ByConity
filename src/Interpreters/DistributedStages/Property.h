#pragma once

#include <Core/Names.h>
#include <Core/Types.h>

namespace DB
{

/**
 * A partition operation divides a relation into disjoint subsets, called partitions.
 * A partition function defines which rows belong to which partitions. Partitioning
 * applies to the whole relation.
 */
class Partitioning
{
public:

    enum class Type : UInt8
    {
        UNKNOWN = 0,
        LOCAL,
        DISTRIBUTED,
    };
    
    Partitioning(const Names & columns_ = {})
        : columns(columns_){}

    const Names & getPartitioningColumns() const { return columns; }

private:
    Names columns;
};

}
