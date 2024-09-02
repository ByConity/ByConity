#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>
#include <Parsers/ASTIdentifier.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <Common/assert_cast.h>

namespace DB
{

String distributionToString(RuntimeFilterDistribution distribution)
{
    switch (distribution)
    {
        case RuntimeFilterDistribution::LOCAL:
            return "Local";
        case RuntimeFilterDistribution::DISTRIBUTED:
            return "Distributed";
        default:
            return "UNKNOWN";
    }
}

String bypassTypeToString(BypassType type)
{
    switch (type)
    {
        case BypassType::NO_BYPASS:
            return "NO_BYPASS";
        case BypassType::BYPASS_EMPTY_HT:
            return "BYPASS_EMPTY_HT";
        case BypassType::BYPASS_LARGE_HT:
            return "BYPASS_LARGE_HT";
        default:
            return "UNKNOWN";
    }
}


RuntimeFilterBuilder::RuntimeFilterBuilder(const Settings & settings, const LinkedHashMap<String, RuntimeFilterBuildInfos> & runtime_filters_)
    : runtime_filters(runtime_filters_)
    , enable_range_cover(settings.enable_range_cover)
{
    // use min runtime filter id as builder id in coordinator merge.
    builder_id = std::min_element(runtime_filters.begin(), runtime_filters.end(), [](const auto & lhs, const auto & rhs) {
                     return lhs.second.id < rhs.second.id;
                 })->second.id;
}

RuntimeFilterData RuntimeFilterBuilder::merge(std::map<UInt32, RuntimeFilterData> && data_sets) const
{
    RuntimeFilterData res;
    // if all empty bypass
    bool all_empty = true;
    for (auto & ds : data_sets)
        all_empty = ds.second.bypass == BypassType::BYPASS_EMPTY_HT && all_empty;

    if (all_empty)
    {
        res.bypass = BypassType::BYPASS_EMPTY_HT;
        return res;
    }

    // should be no large bypass
    bool all_ready = true;
    for (auto & ds : data_sets)
        all_ready = ds.second.bypass != BypassType::BYPASS_LARGE_HT && all_ready;

    if (!all_ready)
    {
        // some bypass, some not, need bypass final
        res.bypass = BypassType::BYPASS_LARGE_HT;
        return res;
    }

    /// if all pre enlarged, use pre enlarge else use the shuffle-aware grf
    std::map<RuntimeFilterId, bool> is_all_pre_enlarged;
    for (const auto & data : data_sets)
    {
        for (const auto & rf : data.second.runtime_filters)
        {
            if (rf.second.is_bf && is_all_pre_enlarged.contains(rf.first))
                is_all_pre_enlarged[rf.first] = is_all_pre_enlarged[rf.first] && rf.second.bloom_filter->is_pre_enlarged;
            else
                is_all_pre_enlarged[rf.first] = !rf.second.is_bf ||  rf.second.bloom_filter->is_pre_enlarged; /// value set treat as enlarged
        }
    }

    std::vector<RuntimeFilterId> invalid_ids;
    for (auto && [pid, rfs] : data_sets)
    {
        if (rfs.bypass == BypassType::BYPASS_EMPTY_HT)
            continue ; // it's ok if some worker's rf is empty

        for (auto && [rf_id, rfv] : rfs.runtime_filters)
        {
            if (res.runtime_filters.contains(rf_id))
            {
                if (res.runtime_filters[rf_id].is_bf != rfv.is_bf)
                {
                    /// try merge the value set back to bloom filter, only support numeric type
                    bool merge_value_set_success = false;
                    if (res.runtime_filters[rf_id].is_bf)
                    {
                        if (res.runtime_filters[rf_id].bloom_filter->num_partitions > 0) /// shuffle's component cannot merge value set
                        {
                            invalid_ids.push_back(rf_id);
                            continue;
                        }

                        if (res.runtime_filters[rf_id].bloom_filter->has_min_max && rfv.values_set->has_min_max)
                        {
                            res.runtime_filters[rf_id].bloom_filter->mergeValueSet(*rfv.values_set);
                            merge_value_set_success = true;
                        }
                    }
                    else
                    {
                        if (rfv.bloom_filter->has_min_max && res.runtime_filters[rf_id].values_set->has_min_max)
                        {
                            rfv.bloom_filter->mergeValueSet(*res.runtime_filters[rf_id].values_set);
                            res.runtime_filters[rf_id] = std::move(rfv);
                            merge_value_set_success = true;
                        }
                    }

                    // for global rf, if different worker generate different rf, this rf should be invalid
                    if (!merge_value_set_success)
                        invalid_ids.push_back(rf_id);
                    continue ;
                }

                if (rfv.is_bf)
                {
                    if (is_all_pre_enlarged.at(rf_id))
                        res.runtime_filters[rf_id].bloom_filter->mergeInplace(std::move(*rfv.bloom_filter));
                    else
                        res.runtime_filters[rf_id].bloom_filter->concatInplace(std::move(*rfv.bloom_filter));
                }
                else
                {
                    res.runtime_filters[rf_id].values_set->merge(*rfv.values_set);
                }
            }
            else
            {
                res.runtime_filters.emplace(rf_id, std::move(rfv));
                if (!is_all_pre_enlarged.at(rf_id) && res.runtime_filters.at(rf_id).is_bf)
                {
                    res.runtime_filters[rf_id].bloom_filter->initForConcat();
                }
            }
        }
    }

    for (const auto id: invalid_ids)
    {
        res.runtime_filters.erase(id);
    }

    data_sets.clear();
    return res;
}

std::unordered_map<RuntimeFilterId, InternalDynamicData> RuntimeFilterBuilder::extractDistributedValues(RuntimeFilterData && data) const
{
    std::unordered_map<RuntimeFilterId, InternalDynamicData> res;
    if (data.bypass == BypassType::BYPASS_LARGE_HT)
    {
        for (const auto & rf : runtime_filters)
        {
            if (rf.second.distribution != RuntimeFilterDistribution::DISTRIBUTED)
                continue;
            InternalDynamicData d;
            d.bypass = BypassType::BYPASS_LARGE_HT;
            res.emplace(rf.second.id, d);
        }

        return res; // no rf generate, abort
    }

    if (data.bypass == BypassType::BYPASS_EMPTY_HT)
    {
        for (const auto & rf : runtime_filters)
        {
            if (rf.second.distribution != RuntimeFilterDistribution::DISTRIBUTED)
                continue;
            InternalDynamicData d;
            d.bypass = BypassType::BYPASS_EMPTY_HT;
            res.emplace(rf.second.id, d);
        }

        return res; // all empty
    }

    auto & rf_map = data.runtime_filters;

    for (const auto & runtime_filter : runtime_filters)
    {
        if (runtime_filter.second.distribution != RuntimeFilterDistribution::DISTRIBUTED)
            continue;

        auto id = runtime_filter.second.id;
        InternalDynamicData filter;

        if (rf_map.contains(id))
        {
            if (rf_map[id].is_bf)
            {
                if (enable_range_cover && data.runtime_filters[id].bloom_filter->isRangeMatch())
                {
                    Array array;
                    array.emplace_back(data.runtime_filters[id].bloom_filter->min());
                    array.emplace_back(data.runtime_filters[id].bloom_filter->max());
                    filter.range = std::move(array);
                    res[id] = std::move(filter);
                    continue;
                }

                WriteBufferFromOwnString buffer;
                data.runtime_filters[id].bloom_filter->serializeToBuffer(buffer);
                filter.bf = std::move(buffer.str());
                if (!enable_range_cover && data.runtime_filters[id].bloom_filter->has_min_max)
                {
                    Array array;
                    array.emplace_back(data.runtime_filters[id].bloom_filter->min());
                    array.emplace_back(data.runtime_filters[id].bloom_filter->max());
                    filter.range = std::move(array);
                }
                res[id] = std::move(filter);
            }
            else
            {
                if (enable_range_cover && data.runtime_filters[id].values_set->isRangeMatch())
                {
                    Array array;
                    array.emplace_back(data.runtime_filters[id].values_set->min);
                    array.emplace_back(data.runtime_filters[id].values_set->max);
                    filter.range = std::move(array);
                    res[id] = std::move(filter);
                    continue;
                }

                Array array;
                for (auto & v : data.runtime_filters[id].values_set->set)
                {
                    array.emplace_back(v);
                }
                filter.set = std::move(array);

                if (!enable_range_cover && data.runtime_filters[id].values_set->has_min_max)
                {
                    Array arr;
                    arr.emplace_back(data.runtime_filters[id].values_set->min);
                    arr.emplace_back(data.runtime_filters[id].values_set->max);
                    filter.range = std::move(arr);
                }
                res[id] = std::move(filter);
            }
        }
        else
            res[id] = InternalDynamicData{};
    }

    return res;
}


void RuntimeFilterVal::deserialize(ReadBuffer & buf)
{
    readBinary(is_bf, buf);
    if (is_bf)
    {
        bloom_filter = std::make_unique<BloomFilterWithRange>();
        bloom_filter->deserialize(buf);
    }
    else
    {
        values_set = std::make_unique<ValueSetWithRange>();
        values_set->deserialize(buf);
    }
}

void RuntimeFilterVal::serialize(WriteBuffer & buf) const
{
    writeBinary(is_bf, buf);
    if (is_bf)
        bloom_filter->serializeToBuffer(buf);
    else
        values_set->serializeToBuffer(buf);
}

String RuntimeFilterVal::dump() const
{
    std::stringstream ss;
    if (bloom_filter)
        ss << "bloom filter";
    else if (values_set)
        ss << "values set"; 
    else
        ss << "empty";
    return ss.str();
}

void RuntimeFilterData::deserialize(ReadBuffer & buf)
{
    UInt8 t;
    readBinary(t, buf);
    bypass = BypassType(t);
    size_t size;
    readBinary(size, buf);
    for (size_t i = 0; i < size; ++i)
    {
        RuntimeFilterId id;
        readBinary(id, buf);
        RuntimeFilterVal val;
        val.deserialize(buf);
        runtime_filters.emplace(id, std::move(val));
    }
}

void RuntimeFilterData::serialize(WriteBuffer & buf) const
{
    writeBinary(UInt8(bypass), buf);
    writeBinary(runtime_filters.size(), buf);
    for (const auto & bf_filter : runtime_filters)
    {
        writeBinary(bf_filter.first, buf);
        bf_filter.second.serialize(buf);
    }
}

bool RuntimeFilterData::isBloomFilter(DB::RuntimeFilterId id) const
{
    return runtime_filters.contains(id) && runtime_filters.at(id).is_bf;
}

bool RuntimeFilterData::isValueSet(DB::RuntimeFilterId id) const
{
    return runtime_filters.contains(id) && !runtime_filters.at(id).is_bf;
}

String RuntimeFilterData::dump() const
{
    return "total rfs:" +  std::to_string(runtime_filters.size());
}

void RuntimeFilterBuildInfos::toProto(Protos::RuntimeFilterBuildInfos & proto) const
{
    proto.set_id(id);
    proto.set_distribution(RuntimeFilterDistributionConverter::toProto(distribution));
}

RuntimeFilterBuildInfos RuntimeFilterBuildInfos::fromProto(const Protos::RuntimeFilterBuildInfos & proto)
{
    auto id = proto.id();
    auto distribution = RuntimeFilterDistributionConverter::fromProto(proto.distribution());
    return RuntimeFilterBuildInfos(id, distribution);
}
}
