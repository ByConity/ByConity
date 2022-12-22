#pragma once

#include <Core/Types.h>


namespace DB
{

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();

    String UUIDToString(const UUID & uuid);
    UUID toUUID(const String & uuid_str);

    const UUID Nil{};
}

}
