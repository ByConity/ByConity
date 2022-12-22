#pragma once

#include <Core/Types.h>

namespace DB::Status
{
#define STATUS_DELETE 0x01ull
#define STATUS_INACTIVE 0x02ull
#define STATUS_DETACH 0x04ull

UInt64 setDelete(const UInt64 & status);
bool isDeleted(const UInt64 & status);

UInt64 setDetached(const UInt64 & status);
UInt64 setAttached(const UInt64 & status);
bool isDetached(const UInt64 & status);

UInt64 setInActive(const UInt64 & status, const bool is_active);
bool isInActive(const UInt64 & status);
}
