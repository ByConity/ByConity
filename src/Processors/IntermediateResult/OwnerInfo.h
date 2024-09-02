#pragma once

#include <Core/Types.h>
#include <fmt/core.h>

namespace DB
{

struct OwnerInfo
{
public:
    OwnerInfo() = default;
    OwnerInfo(const String & name_, time_t modification_time_) : name(name_), modification_time(modification_time_) {}

    bool operator==(const OwnerInfo & other) const
    {
        return name == other.name && modification_time == other.modification_time;
    }
    String toString() const
    {
        return fmt::format("{}-{}", name, modification_time);
    }
    bool empty() const
    {
        return name.empty();
    }
    void reset()
    {
        name.clear();
    }

private:
    String name;
    // If a part in unique table, update twice in succession within 1 second, 
    // modification_time may be the same, so we need a more precise time(only in memory)
    time_t modification_time{0};
};

}
