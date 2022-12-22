#pragma once

#include <Interpreters/RuntimeFilter/RuntimeFilter.h>
#include <Processors/ISimpleTransform.h>
#include <common/logger_useful.h>

namespace DB
{
class RuntimeFilterConsumer;

class BuildRuntimeFilterTransform : public ISimpleTransform
{
public:
    BuildRuntimeFilterTransform(
        const Block & header,
        ContextPtr context,
        const Block & join_keys,
        std::unordered_map<String, String> join_column_map,
        bool enable_bloom_filter,
        bool enable_range_filter,
        std::unordered_map<String, std::vector<String>> multiple_alias,
        std::optional<std::shared_ptr<RuntimeFilterConsumer>> consumer_ = {});

    String getName() const override { return "BuildRuntimeFilter"; }

protected:
    Status prepare() override;
    void work() override;
    void transform(Chunk & chunk) override;

private:
    RuntimeFilterPtr rf;
    std::shared_ptr<RuntimeFilterConsumer> consumer;
    bool input_finish = false;
    bool finished = false;
    Poco::Logger * log;
};

class RuntimeFilterConsumer
{
public:
    RuntimeFilterConsumer(
        std::string query_id,
        UInt32 filter_id_,
        size_t local_stream_parallel_,
        size_t parallel_,
        AddressInfo coordinator_address_,
        AddressInfo current_address_);

    void addFinishRuntimeFilter(RuntimeFilterPtr runtime_filter);

private:
    RuntimeFilterPtr mergeRuntimeFilter();
    void transferRuntimeFilter(const RuntimeFilterPtr & runtime_filter);

    const std::string query_id;
    const size_t filter_id;
    const size_t local_stream_parallel;
    const size_t parallel;
    const AddressInfo coordinator_address;
    const AddressInfo current_address;

    std::mutex mutex;
    RuntimeFilterPtrs runtime_filters{};

    Stopwatch timer;
    Poco::Logger * log;
};
}
