#pragma once

#include <Core/Types.h>

namespace DB
{
enum class LockMode : UInt32
{
    NONE = 0,
    IS = 1,
    IX = 2,
    S = 3,
    X = 4,
    SIZE = 5,
};

template <typename E>
constexpr auto to_underlying(E e) noexcept
{
    return static_cast<std::underlying_type_t<E>>(e);
}

static constexpr auto LockModeSize = to_underlying(LockMode::SIZE);

bool conflicts(LockMode newMode, UInt32 currentModes);

inline UInt32 modeMask(LockMode mode)
{
    return 1 << to_underlying(mode);
}

constexpr auto toString(LockMode mode)
{
    switch (mode)
    {
        case LockMode::NONE:
            return "None";
        case LockMode::IS:
            return "IS";
        case LockMode::IX:
            return "IX";
        case LockMode::S:
            return "S";
        case LockMode::X:
            return "X";
        default:
            return "Unknown";
    }
}

String lockModesToDebugString(UInt32 modes);

enum class LockStatus : UInt32
{
    LOCK_INIT = 0,
    LOCK_OK,
    LOCK_WAITING,
    LOCK_TIMEOUT,
};

enum class LockLevel : UInt32
{
    TABLE = 0,
    BUCKET = 1,
    PARTITION = 2,
    SIZE = 3,
};

static constexpr auto LockLevelSize = to_underlying(LockLevel::SIZE);

constexpr auto toString(LockLevel level)
{
    switch (level)
    {
        case LockLevel::TABLE:
            return "Table";
        case LockLevel::BUCKET:
            return "Bucket";
        case LockLevel::PARTITION:
            return "Partition";
        default:
            return "Unknown";
    }
}
}
