#pragma once


namespace DB
{

enum class DataDestinationType
{
    DISK,
    VOLUME,
    BYTECOOL,
    TABLE,
    DELETE,
    SHARD,
};

}
