#include <Storages/Kafka/ReadMemoryTableMode.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_TYPE_OF_FIELD;
}

const char *readModeToString(ReadMemoryTableMode mode)
{
    switch (mode)
    {
        case ReadMemoryTableMode::ALL:
            return "ALL";
        case ReadMemoryTableMode::PART:
            return "PART";
        case ReadMemoryTableMode::SKIP:
            return "SKIP";
    }
}

ReadMemoryTableMode stringToReadMode(String mode)
{
    if (mode == "ALL")
        return ReadMemoryTableMode::ALL;
    else if (mode == "PART")
        return ReadMemoryTableMode::PART;
    else if (mode == "SKIP")
        return ReadMemoryTableMode::SKIP;
    else
        throw Exception("Unknown read memory table mode " + mode, ErrorCodes::BAD_TYPE_OF_FIELD);
}

}
