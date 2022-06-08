#include <Common/Status.h>

namespace DB::Status
{

UInt64 setDelete(const UInt64 & status) { return status | STATUS_DELETE; }

bool isDeleted(const UInt64 & status) { return status & STATUS_DELETE; }


UInt64 setDetached(const UInt64 & status)
{
    return status | STATUS_DETACH;
}

UInt64 setAttached(const UInt64 & status)
{
    return (~STATUS_DETACH) & status;
}


bool isDetached(const UInt64 & status) { return status & STATUS_DETACH; }

UInt64 setInActive(const UInt64 & status, const bool is_active)
{
    if (is_active)
        return (~STATUS_INACTIVE) & status;
    else
        return status | STATUS_INACTIVE;
}

bool isInActive(const UInt64 & status) { return status & STATUS_INACTIVE; }

}
