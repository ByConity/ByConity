#pragma once

#include <memory>

#include "CloudServices/ParallelReadRequestResponse.h"

namespace DB
{

/// The main class to spread tasks across replicas dynamically
class ParallelReplicasReadingCoordinator
{
public:
    class ImplInterface;

    explicit ParallelReplicasReadingCoordinator(size_t replicas_count_);
    ~ParallelReplicasReadingCoordinator();

    ParallelReadResponse handleRequest(ParallelReadRequest request);

    /// for source
    void addSplit();
    void finish();

private:
    void initialize();

    CoordinationMode mode{CoordinationMode::Default};
    size_t replicas_count{0};
    std::atomic<bool> initialized{false};
    std::unique_ptr<ImplInterface> pimpl;
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
