#pragma once

#include <memory>

#include "CloudServices/ParallelReadRequestResponse.h"

namespace DB
{

/// The main class to spread tasks across replicas dynamically
class IDistributedReadingCoordinator
{
public:
    IDistributedReadingCoordinator() = default;
    virtual ~IDistributedReadingCoordinator() = default;

    virtual ParallelReadResponse handleRequest(ParallelReadRequest request) = 0;
    //// handle initial request
    virtual void finish() = 0;
};

}
