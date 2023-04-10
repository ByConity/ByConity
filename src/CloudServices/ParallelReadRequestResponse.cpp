#include "CloudServices/ParallelReadRequestResponse.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteHelpers.h"

namespace DB
{
void ParallelReadRequest::serialize(WriteBuffer & out) const
{
    writeStringBinary(worker_id, out);
    writeIntBinary(min_weight, out);
}

void ParallelReadRequest::deserialize(ReadBuffer & in)
{
    readStringBinary(worker_id, in);
    readIntBinary(min_weight, in);
}

void ParallelReadResponse::serialize(WriteBuffer & out) const
{
    writeBoolText(finish, out);
    split.serialize(out);
}

void ParallelReadResponse::deserialize(ReadBuffer & in)
{
    readBoolText(finish, in);
    split.deserialize(in);
}

}
