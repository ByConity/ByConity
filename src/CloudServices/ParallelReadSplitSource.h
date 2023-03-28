#pragma once

#include "Common/ConcurrentBoundedQueue.h"
#include "CloudServices/ParallelReadSplit.h"
namespace DB
{

struct ParallelReadSplitSource
{
    std::shared_ptr<ConcurrentBoundedQueue<IParallelReadSplit>> queue;
    bool hasNext();
    IParallelReadSplit next();
};

}
