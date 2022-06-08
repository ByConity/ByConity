#include <MergeTreeCommon/InsertionLabel.h>

#include <Catalog/MetastoreProxy.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

InsertionLabel::InsertionLabel(UUID uuid, const std::string & name_, UInt64 txn_id_) : table_uuid(uuid), txn_id(txn_id_)
{
    validateName(name_);
    name = name_;
    create_time = time(nullptr);
}

void InsertionLabel::validateName(const std::string & name)
{
    for (auto & c : name)
    {
        if (!std::isalnum(c) && c != '#' && c != '-')
            throw Exception("Invalid label name, only allowed alphanumeric chars and two special chars {#, -}", ErrorCodes::BAD_ARGUMENTS);
    }
}

std::string InsertionLabel::serializeValue() const
{
    WriteBufferFromOwnString out;
    writeIntBinary(txn_id, out);
    writeIntBinary(create_time, out);
    UInt8 temp = status;
    writeIntBinary(temp, out);
    return out.str();
}

void InsertionLabel::parseValue(const std::string & data)
{
    ReadBufferFromString in(data);
    readValue(in);
}

void InsertionLabel::readValue(ReadBuffer & in)
{
    readIntBinary(txn_id, in);
    readIntBinary(create_time, in);
    UInt8 temp{0};
    readIntBinary(temp, in);
    status = static_cast<Status>(temp);
}
}
