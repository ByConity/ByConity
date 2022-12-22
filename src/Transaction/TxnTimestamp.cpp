#include "TxnTimestamp.h"
#include <sstream>

namespace DB
{
std::string TxnTimestamp::toString() const
{
    std::ostringstream oss;
    oss << _ts;
    return oss.str();
}

}
