#include <Storages/SelectQueryInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

void InputOrderInfo::serialize(WriteBuffer & buf) const
{
    serializeSortDescription(order_key_prefix_descr, buf);
    writeBinary(direction, buf);
}

void InputOrderInfo::deserialize(ReadBuffer & buf)
{
    deserializeSortDescription(order_key_prefix_descr, buf);
    readBinary(direction, buf);
}


}
