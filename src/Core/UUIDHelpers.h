#pragma once

#include <Core/UUID.h>
#include <Common/thread_local_rng.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace UUIDHelpers
{
    inline UUID generateV4()
    {
        UInt128 res{thread_local_rng(), thread_local_rng()};
        res.items[0] = (res.items[0] & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        res.items[1] = (res.items[1] & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        return UUID{res};
    }

    inline String UUIDToString(const UUID & uuid)
    {
        String uuid_str;
        WriteBufferFromString buff(uuid_str);
        writeUUIDText(uuid, buff);
        return uuid_str;
    }

    // const UUID Nil = UUID(UInt128{0, 0});
}

}
