#pragma once

#include <Core/Types.h>

namespace DB
{

enum class ExchangeMode : UInt8
{
    UNKNOWN = 0,
    LOCAL_NO_NEED_REPARTITION, // for global join, if we want to increase the parallel size, just split it
    LOCAL_MAY_NEED_REPARTITION, // for local join, if we want to increase the parallel size, we need repartition
    REPARTITION,
    BROADCAST,
    GATHER
};

String exchangeModeToString(const ExchangeMode & exchange_mode);

}
