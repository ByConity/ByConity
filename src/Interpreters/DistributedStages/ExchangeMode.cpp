#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <sstream>

namespace DB
{

String exchangeModeToString(const ExchangeMode & exchange_mode)
{
    std::ostringstream ostr;

    switch(exchange_mode)
    {
        case ExchangeMode::UNKNOWN:
            ostr << "UNKNOWN";
            break;
        case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
            ostr << "LOCAL_NO_NEED_REPARTITION";
            break;
        case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            ostr << "LOCAL_MAY_NEED_REPARTITION";
            break;
        case ExchangeMode::REPARTITION:
            ostr << "REPARTITION";
            break;
        case ExchangeMode::BROADCAST:
            ostr << "BROADCAST";
            break;
        case ExchangeMode::GATHER:
            ostr << "GATHER";
            break;
    }

    return ostr.str();
}


}
