#pragma once

#include <Common/Logger.h>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>
#include <common/types.h>
#include <parallel_hashmap/phmap.h>
#include <bthread/mutex.h>
namespace DB
{

class MPPQueryCoordinator;

class MPPQueryManager final : private boost::noncopyable
{
public:
    static MPPQueryManager & instance()
    {
        static MPPQueryManager instance;
        return instance;
    }

void registerQuery(const String & query_id, std::weak_ptr<MPPQueryCoordinator> coordinator);

void clearQuery(const String & query_id);

std::shared_ptr<MPPQueryCoordinator> getCoordinator(const String & query_id);

private:
    MPPQueryManager() = default;
    using CoordinatorWeakPtr = std::weak_ptr<MPPQueryCoordinator>;
    using MPPCoordinatorMap = phmap::parallel_flat_hash_map<
    String,
    CoordinatorWeakPtr,
    phmap::priv::hash_default_hash<String>,
    phmap::priv::hash_default_eq<String>,
    std::allocator<std::pair<String, CoordinatorWeakPtr>>,
    8,
    bthread::Mutex>;
    MPPCoordinatorMap coordinator_map;
    LoggerPtr log {getLogger("MPPQueryManager")};
};

}
