#pragma once

#include <Poco/Logger.h>
#include "CloudServices/IDistributedReadingCoordinator.h"
#include "CloudServices/ParallelReadRequestResponse.h"
#include "Storages/Hive/HiveDataPart_fwd.h"

#include <unordered_map>

namespace DB
{

class WorkerGroupHandleImpl;

class HiveDistributedReadingCoordinator : public IDistributedReadingCoordinator
{
public:
    /// decide parts go to which worker
    using Allocator = std::function<size_t(const HiveDataPartCNCHPtr & part, size_t)>;
    static size_t consistentHashAllocator(const HiveDataPartCNCHPtr & part, size_t replicas_count_);

    explicit HiveDistributedReadingCoordinator(
        const std::shared_ptr<WorkerGroupHandleImpl> & worker_group,
        Allocator alloc = consistentHashAllocator,
        bool enable_work_stealing_ = false);

    ~HiveDistributedReadingCoordinator() override;

    ParallelReadResponse handleRequest(ParallelReadRequest request) override;

    /// for hive part generator
    void finish() override;
    void addParts(const HiveDataPartsCNCHVector & pending_parts);

private:
    struct State;
    std::vector<std::unique_ptr<State>> stats;

    Allocator allocator;
    const bool enable_work_stealing;
    using KeyToIndex = std::unordered_map<String, size_t>;
    KeyToIndex key_index;
    Poco::Logger * log {&Poco::Logger::get("HiveDistributedReadingCoordinator")};
};

}
