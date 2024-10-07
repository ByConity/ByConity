#pragma once
#include <Common/Logger.h>
#include <Common/LinkedHashMap.h>
#include <Processors/ISimpleTransform.h>
#include <common/logger_useful.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>

namespace DB
{

class RuntimeFilterConsumer
{
public:
    RuntimeFilterConsumer(
        std::shared_ptr<RuntimeFilterBuilder> builder_,
        std::string query_id,
        size_t local_stream_parallel_,
        size_t parallel_,
        AddressInfo coordinator_address_,
        UInt32 parallel_id_);

    const LinkedHashMap<String, RuntimeFilterBuildInfos> & getRuntimeFilters() const { return builder->getRuntimeFilters(); }
    void addFinishRF(BloomFilterWithRangePtr && bf_ptr, RuntimeFilterId  id, bool is_local);
    void addFinishRF(ValueSetWithRangePtr && vs_ptr, RuntimeFilterId id, bool is_local);
    void bypass(BypassType type);
    void finalize();
    bool isDistributed(const String & name) const
    {
        return !builder->isLocal(name);
    }

   void fixParallel(size_t parallel)
   {
        local_stream_parallel = parallel;
        build_params_blocks.resize(local_stream_parallel);
    }
    size_t getLocalSteamParallel() const
    {
        return local_stream_parallel;
    }
    size_t getParallelWorkers() const
    {
        return parallel_workers;
    }

    bool addBuildParams(size_t ht_size, const BlocksList * blocks);
    std::vector<const BlocksList *> && buildParamsBlocks()
    {
        return std::move(build_params_blocks);
    }

    size_t totalHashTableSize()
    {
        return ht_sizes.load(std::memory_order_relaxed);
    }

    bool isBypassed()
    {
        return is_bypassed.load(std::memory_order_relaxed);
    }

private:
    void transferRuntimeFilter(RuntimeFilterData && data);

    const std::shared_ptr<RuntimeFilterBuilder> builder;
    const std::string query_id;
    mutable size_t local_stream_parallel;
    const size_t parallel_workers;
    const AddressInfo coordinator_address;
    const UInt32 parallel_id;

    bthread::Mutex mutex;
    RuntimeFilterData global_rf_data;
    std::vector<const BlocksList *> build_params_blocks;
    std::atomic_size_t ht_sizes = 0;
    std::atomic_int num_partial = 0;
    std::atomic_bool is_bypassed = false;

    Stopwatch timer;
    LoggerPtr log;
};
}
