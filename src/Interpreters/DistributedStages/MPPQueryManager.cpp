#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <boost/noncopyable.hpp>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/MPPQueryCoordinator.h>
#include <Interpreters/DistributedStages/MPPQueryManager.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
}

void MPPQueryManager::registerQuery(const String & query_id, std::weak_ptr<MPPQueryCoordinator> coordinator)
{
    auto res = coordinator_map.try_emplace(query_id, std::move(coordinator));
    if (!res.second)
    {
        throw Exception(
            "Mpp query with id = " + query_id + " is already running and can't be stopped",
            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
    }
}

void MPPQueryManager::clearQuery(const String & query_id)
{
    auto res = coordinator_map.erase(query_id);
    LOG_TRACE(log, "clear query: {} with res: {}", query_id, res);
}

std::shared_ptr<MPPQueryCoordinator> MPPQueryManager::getCoordinator(const String & query_id)
{
    std::shared_ptr<MPPQueryCoordinator> res;
    coordinator_map.if_contains(query_id, [&res](auto & pair) { res = pair.second.lock(); });
    return res;
}
}
