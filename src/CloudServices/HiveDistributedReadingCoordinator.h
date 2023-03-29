#pragma once

#include "CloudServices/IDistributedReadingCoordinator.h"
#include "CloudServices/ParallelReadRequestResponse.h"
#include "Storages/Hive/HiveDataPart_fwd.h"

namespace DB
{

class HiveDistributedReadingCoordinator : public IDistributedReadingCoordinator
{
public:
    /// decide parts go to which worker
    using Allocator = std::function<size_t(const HiveDataPartCNCHPtr & part, size_t)>;
    static size_t consistentHashAllocator(const HiveDataPartCNCHPtr & part, size_t replicas_count_);

    explicit HiveDistributedReadingCoordinator(
        size_t replicas_count_, size_t split_len = std::numeric_limits<size_t>::max(), Allocator alloc = consistentHashAllocator);

    ParallelReadResponse handleRequest(ParallelReadRequest request) override;

    /// for hive part generator
    void finish() override;
    void addParts(HiveDataPartsCNCHVector & pending_parts);

private:
    struct State;
    std::vector<std::unique_ptr<State>> stats;

    size_t replicas_count;
    size_t split_len;
    Allocator allocator;
};

}
