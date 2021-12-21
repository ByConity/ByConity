#include <Storages/SelectQueryInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTSerDerHelper.h>

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

void SelectQueryInfo::serialize(WriteBuffer & buf) const
{
    serializeAST(query, buf);
    serializeAST(view_query, buf);
}

void SelectQueryInfo::deserialize(ReadBuffer & buf)
{
    query = deserializeAST(buf);
    view_query = deserializeAST(buf);
}

}
