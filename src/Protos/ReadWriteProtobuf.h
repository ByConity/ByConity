#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/hash.h>

namespace DB
{

UInt64 sipHash64Protobuf(const google::protobuf::Message & proto);

}
