#pragma once

#include <Core/Types.h>

#include <memory>

namespace DB
{
using PlanNodeId = UInt32;

class PlanNodeIdAllocator;
using PlanNodeIdAllocatorPtr = std::shared_ptr<PlanNodeIdAllocator>;

class PlanNodeIdAllocator
{
public:
    PlanNodeIdAllocator() : next_id(0) { }

    explicit PlanNodeIdAllocator(PlanNodeId next_id_) : next_id(next_id_) { }

    PlanNodeId nextId() { return next_id++; }

private:
    PlanNodeId next_id;
};
}
