#pragma once

#include <CloudServices/CnchServerClient.h>
#include <Common/RpcClientPool.h>

namespace DB
{
template class RpcClientPool<CnchServerClient>;
}
