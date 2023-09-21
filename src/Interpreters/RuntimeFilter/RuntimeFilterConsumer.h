#pragma once

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
        AddressInfo current_address_);

    const LinkedHashMap<String, RuntimeFilterBuildInfos> & getRuntimeFilters() const { return builder->getRuntimeFilters(); }
    void addFinishRuntimeFilter(RuntimeFilterData && data, bool is_local);
    void addFinishRF(BloomFilterWithRangePtr && bf_ptr, RuntimeFilterId  id, bool is_local);
    void addFinishRF(ValueSetWithRangePtr && vs_ptr, RuntimeFilterId id, bool is_local);
    void bypass(RuntimeFilterId id, bool is_local, BypassType type);
    bool isBloomFilter(RuntimeFilterId id) const;
    bool isValueSet(RuntimeFilterId id) const;
    bool isDistributed(const String & name) const
    {
        return !builder->isLocal(name);
    }

    void fixParallel(size_t parallel_) {local_stream_parallel *= parallel_;}
    size_t getLocalSteamParallel()
    {
        return local_stream_parallel;
    }

private:
    void transferRuntimeFilter(RuntimeFilterData && data);

    const std::shared_ptr<RuntimeFilterBuilder> builder;
    const std::string query_id;
    mutable size_t local_stream_parallel;
    const size_t parallel;
    const AddressInfo coordinator_address;
    const AddressInfo current_address;

    std::mutex mutex;
    std::vector<RuntimeFilterData> runtime_filters{};

    Stopwatch timer;
    Poco::Logger * log;
};
}
