#include "RuntimeFilterTypes.h"

namespace DB
{

void ValueSetWithRange::deserialize(ReadBuffer & buf)
{
    readBinary(has_min_max, buf);
    if (has_min_max)
    {
        readFieldBinary(min, buf);
        readFieldBinary(max, buf);
    }

    size_t size;
    readBinary(size, buf);
    for (size_t i = 0; i < size; ++i)
    {
        Field f;
        readFieldBinary(f, buf);
        set.emplace(std::move(f));
    }
}

void ValueSetWithRange::serializeToBuffer(WriteBuffer & buf)
{
    writeBinary(has_min_max, buf);
    if (has_min_max)
    {
        writeFieldBinary(min, buf);
        writeFieldBinary(max, buf);
    }

    writeBinary(set.size(), buf);
    for (const auto & val : set)
    {
        writeFieldBinary(val, buf);
    }
}

void BloomFilterWithRange::deserialize(ReadBuffer & istr)
{
    readBinary(has_min_max, istr);
    if (has_min_max)
    {
        Field min, max;
        readFieldBinary(min, istr);
        readFieldBinary(max, istr);
        initMinMax(min, max);
    }

    readBinary(has_null, istr);
    readBinary(num_partitions, istr);
    readBinary(is_pre_enlarged, istr);
    if (num_partitions == 0)
    {
        bf.deserialize(istr);
    }
    else
    {
        part_bfs.reserve(num_partitions);
        for (size_t i = 0; i < num_partitions; ++i)
        {
            BlockBloomFilter filter;
            filter.deserialize(istr);
            part_bfs.emplace_back(std::move(filter));
        }
    }

}

void BloomFilterWithRange::serializeToBuffer(WriteBuffer & ostr)
{
    writeBinary(has_min_max, ostr);
    if (has_min_max)
    {
        writeFieldBinary(min_max->getMin(), ostr);
        writeFieldBinary(min_max->getMax(), ostr);
    }

    writeBinary(has_null, ostr);
    writeBinary(num_partitions, ostr);
    writeBinary(is_pre_enlarged, ostr);
    if (num_partitions == 0)
    {
        bf.serializeToBuffer(ostr);
    }
    else
    {
        for(auto & part_bf : part_bfs)
            part_bf.serializeToBuffer(ostr);
    }
}

void BloomFilterWithRange::addFieldKey(const DB::Field & f)
{
    switch (f.getType())
    {
        case Field::Types::UInt64:
        {
            addKey(f.get<UInt64>());
            break ;
        }
        case Field::Types::Int64:
        {
            addKey(f.get<Int64>());
            break ;
        }
        case Field::Types::Int128:
        {
            addKey(f.get<Int128>());
            break ;
        }
        case Field::Types::UInt128:
        {
            addKey(f.get<UInt128>());
            break ;
        }
        case Field::Types::UInt256:
        {
            addKey(f.get<UInt256>());
            break ;
        }
        case Field::Types::Int256:
        {
            addKey(f.get<Int256>());
            break ;
        }
        default:
        {
            throw Exception("addFieldKey unexpected type: " + f.dump() + " info:" + debugString(), ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void BloomFilterWithRange::mergeValueSet(const DB::ValueSetWithRange & valueSetWithRange)
{
    for (const auto & f : valueSetWithRange.set)
    {
        addFieldKey(f);
    }
    if (valueSetWithRange.has_min_max && has_min_max)
    {
        min_max->addFieldKey(valueSetWithRange.min);
        min_max->addFieldKey(valueSetWithRange.max);
    }
}

String BloomFilterWithRange::debugString() const
{
    if (num_partitions == 0)
    {
        if (has_min_max)
            return "ndv:" + std::to_string(bf.ndv) + " min:" + min_max->dumpMin() + " max:" + min_max->dumpMax() ;
        else
            return "ndv:" + std::to_string(bf.ndv);
    }
    else
    {
        auto sum_ndv = [](size_t init, const BlockBloomFilter & r2)
        {
            return init + r2.ndv;
        };

        size_t ndv = std::accumulate(part_bfs.begin(), part_bfs.end(), 0, sum_ndv);
        if (has_min_max)
            return "ndv:" + std::to_string(ndv) + " min:" + min_max->dumpMin() + " max:" + min_max->dumpMax()
                + " num_partition:" + std::to_string(num_partitions);
        else
            return "ndv:" + std::to_string(ndv) + " num_partition:" + std::to_string(num_partitions);
    }
}
} // DB
