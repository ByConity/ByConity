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
    bf.deserialize(istr);
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
    bf.serializeToBuffer(ostr);
}

} // DB
