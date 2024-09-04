#include <DataTypes/Serializations/SerializationNothing.h>
#include <Columns/ColumnNothing.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

void SerializationNothing::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    size_t size = column.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    for (size_t i = 0; i < limit; ++i)
        ostr.write('0');
}

size_t SerializationNothing::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/, bool /*zero_copy_cache_read*/, const UInt8* filter) const
{
    size_t ignored = istr.tryIgnore(limit);
    size_t added = ignored;
    if (filter)
    {
        for (size_t i = 0; i < ignored; ++i)
        {
            if (*(filter + i) == 0)
            {
                --added;
            }
        }
    }
    typeid_cast<ColumnNothing &>(column).addSize(added);
    return ignored;
}

}
