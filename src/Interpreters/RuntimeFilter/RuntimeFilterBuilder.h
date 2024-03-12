#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterTypes.h>
#include <Parsers/IAST.h>
#include <Protos/enum.pb.h>
#include <Common/LinkedHashMap.h>
#include <common/logger_useful.h>
#include "Protos/EnumMacros.h"
#include <bthread/shared_mutex.h>

namespace DB
{
using RuntimeFilterId = UInt32;
using BloomFilterWithRangePtr = std::shared_ptr<BloomFilterWithRange>;
using ValueSetWithRangePtr = std::shared_ptr<ValueSetWithRange>;

namespace Protos
{
    class RuntimeFilterBuildInfos;
}

ENUM_WITH_PROTO_CONVERTER(
    RuntimeFilterDistribution, // enum name
    Protos::RuntimeFilterDistribution, // proto enum message
    (LOCAL),
    (DISTRIBUTED),
    (UNKNOWN));

enum class BypassType : UInt8
{
    NO_BYPASS = 0,   /// Normal case
    BYPASS_EMPTY_HT, /// Empty right table, which can short circuit the left table scan
    BYPASS_LARGE_HT, /// Too large to build runtime filter, same as the runtime filter abort
};

String distributionToString(RuntimeFilterDistribution distribution);
String bypassTypeToString(BypassType type);

struct InternalDynamicData
{
    Field range{};
    Field bf{};
    Field set{};
    BypassType bypass = BypassType::NO_BYPASS;

    String dump() const
    {
        return bypassTypeToString(bypass) + " range:" + range.dump() + " bf:" + bf.dump() + " set:" + set.dump();
    }
};

struct RuntimeFilterBuildInfos
{
    RuntimeFilterId id;
    RuntimeFilterDistribution distribution;

    void toProto(Protos::RuntimeFilterBuildInfos & proto) const;
    static RuntimeFilterBuildInfos fromProto(const Protos::RuntimeFilterBuildInfos & proto);
    RuntimeFilterBuildInfos(RuntimeFilterId id_, RuntimeFilterDistribution distribution_) : id(id_), distribution(distribution_) { }
};

struct RuntimeFilterVal
{
    /* Data: bloom filter and values set is mutual exclusion */
    bool is_bf;
    BloomFilterWithRangePtr bloom_filter;
    ValueSetWithRangePtr values_set; // hash default 1024
    void deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf) const;
    String dump() const;
};

struct RuntimeFilterData
{
    std::unordered_map<RuntimeFilterId, RuntimeFilterVal> runtime_filters;
    BypassType bypass = BypassType::NO_BYPASS;

    bool isBloomFilter(RuntimeFilterId id) const;
    bool isValueSet(RuntimeFilterId id) const;

    void deserialize(ReadBuffer & istr);
    void serialize(WriteBuffer & ostr) const;
    String dump() const;
};

struct DynamicData
{
    DynamicData() :bf_mutex(std::make_shared<bthread::SharedMutex>()) {
    }
    BypassType bypass = BypassType::NO_BYPASS;
    bool is_local = false;
    std::variant<RuntimeFilterVal, InternalDynamicData> data;
    std::shared_ptr<bthread::SharedMutex> bf_mutex;
    BloomFilterWithRangePtr bf;
    String dump()
    {
        if (bypass == BypassType::BYPASS_LARGE_HT)
            return "BYPASS_LARGE_HT";
        else if (bypass == BypassType::BYPASS_EMPTY_HT)
            return "BYPASS_EMPTY_HT";

        std::stringstream ss;
        if (is_local)
            ss << "LOCAL: ";

        if (is_local)
        {
            auto const & d = std::get<RuntimeFilterVal>(data);
            ss << d.dump();
            return ss.str();
        }
        else
        {
            auto const & d = std::get<InternalDynamicData>(data);
            return "range:" + d.range.dump() + " bf:" + d.bf.dump() + " set:" + d.set.dump();
        }
    }
};

class RuntimeFilterBuilder;
using RuntimeFilterBuilderPtr = std::shared_ptr<RuntimeFilterBuilder>;

class RuntimeFilterBuilder
{
public:
    explicit RuntimeFilterBuilder(const Settings & settings, const LinkedHashMap<String, RuntimeFilterBuildInfos> & runtime_filters_);

    UInt32 getId() const { return builder_id; }

    const LinkedHashMap<String, RuntimeFilterBuildInfos> & getRuntimeFilters() const { return runtime_filters; }
    bool isLocal(const String & name) {
        return runtime_filters.at(name).distribution == RuntimeFilterDistribution::LOCAL;
    }

    RuntimeFilterData merge(std::map<UInt32, RuntimeFilterData> && data_sets) const;
    std::unordered_map<RuntimeFilterId, InternalDynamicData> extractDistributedValues(RuntimeFilterData && data) const;

private:
    /**
     * Meta
     */
    LinkedHashMap<String, RuntimeFilterBuildInfos> runtime_filters;
    UInt32 builder_id;
    bool enable_range_cover = false;
};
}
