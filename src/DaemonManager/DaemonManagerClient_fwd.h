#pragma once

#include <memory>
namespace DB
{

namespace DaemonManager
{
    class DaemonManagerClient;
}

template <class T>
class RpcClientPool;

using DaemonManagerClientPtr = std::shared_ptr<DaemonManager::DaemonManagerClient>;
using DaemonManagerClientPool = RpcClientPool<DaemonManager::DaemonManagerClient>;

} /// end namespace DB
