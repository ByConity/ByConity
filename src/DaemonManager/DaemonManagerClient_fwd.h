#pragma once

#include <memory>
#include <Common/RpcClientPool.h>
namespace DB
{

namespace DaemonManager
{
    class DaemonManagerClient;
}

using DaemonManagerClientPtr = std::shared_ptr<DaemonManager::DaemonManagerClient>;
using DaemonManagerClientPool = RpcClientPool<DaemonManager::DaemonManagerClient>;

} /// end namespace DB
