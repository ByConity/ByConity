#pragma once

#include <string>
#include <memory>
#include <Core/UUID.h>

namespace DB
{
class ReadBuffer;

struct InsertionLabel
{
    enum Status : UInt8
    {
        Precommitted = 0,
        Committed = 1,
    };

    UUID table_uuid;
    std::string name;
    UInt64 txn_id{0};
    time_t create_time{0};
    Status status{Precommitted};

    InsertionLabel() = default;
    InsertionLabel(UUID uuid, const std::string & name, UInt64 txn_id = 0);

    void commit() { status = Committed; }

    static void validateName(const std::string & name);

    std::string serializeValue() const;
    void parseValue(const std::string & data);
    void readValue(ReadBuffer & in);
};

using InsertionLabelPtr = std::shared_ptr<InsertionLabel>;

}
