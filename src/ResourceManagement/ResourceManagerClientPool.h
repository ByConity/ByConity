#include <Common/RpcClientPool.h>
#include <ResourceManagement/ResourceManagerClient.h>

namespace DB
{
using ResourceManagerClientPool = RpcClientPool<ResourceManagement::ResourceManagerClient>;
}
