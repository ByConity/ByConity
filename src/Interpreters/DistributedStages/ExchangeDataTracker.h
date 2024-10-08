#pragma once

#include <Common/Logger.h>
#include <cstddef>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Protos/plan_segment_manager.pb.h>
#include <common/types.h>
#include "Interpreters/DistributedStages/AddressInfo.h"

#include <Interpreters/DistributedStages/PlanSegment.h>

namespace DB
{

/// Output of one exchange sink.
struct ExchangeStatus
{
    ExchangeStatus() = default;
    bool empty() const
    {
        return status.empty();
    }

    // Address of sink instance.
    AddressInfo worker_addr;
    // Output length of each partition.
    std::vector<size_t> status;
};

std::vector<std::pair<size_t, ExchangeStatus>> fromSenderMetrics(const Protos::SenderMetrics & sender_metrics);

/// Output of one exchange.
struct ExchangeStatuses
{
    explicit ExchangeStatuses(size_t parallel_size)
    {
        sink_statuses.resize(parallel_size);
    }
    ExchangeStatuses() = default;
    ~ExchangeStatuses() = default;
    ExchangeStatuses(const ExchangeStatuses &) = default;
    ExchangeStatuses & operator=(const ExchangeStatuses &) = default;
    ExchangeStatuses(const ExchangeStatuses && other)
    {
        sink_statuses = std::move(other.sink_statuses);
    }
    ExchangeStatuses & operator=(ExchangeStatuses && other)
    {
        sink_statuses = std::move(other.sink_statuses);
        return *this;
    }

    bool empty() const
    {
        return sink_statuses.empty();
    }

    void addStatus(UInt64 parallel_index, const ExchangeStatus & status)
    {
        sink_statuses[parallel_index] = std::move(status);
    }

    void removeStatus(UInt64 parallel_index, const AddressInfo & addr)
    {
        if (!sink_statuses[parallel_index].empty() && sink_statuses[parallel_index].worker_addr == addr)
            sink_statuses[parallel_index] = ExchangeStatus{};
    }

    const std::vector<ExchangeStatus> & getStatusesRef() const
    {
        return sink_statuses;
    }

    // Exchange status of each sink instance.
    std::vector<ExchangeStatus> sink_statuses;
};

// Struct indicates an exchange (all data exchange between two plan segments).
struct ExchangeKey
{
    String query_id;
    UInt64 exchange_id;

    friend bool operator==(const ExchangeKey & left, const ExchangeKey & right)
    {
        return left.query_id == right.query_id && left.exchange_id == right.exchange_id;
    }
    friend bool operator!=(const ExchangeKey & left, const ExchangeKey & right)
    {
        return !(left == right);
    }

    class Hash
    {
    public:
        size_t operator()(const ExchangeKey & key) const
        {
            return std::hash<std::string_view>{}(key.query_id) + static_cast<size_t>(key.exchange_id);
        }
    };
};

class ExchangeStatusTracker : public WithContext
{
public:
    explicit ExchangeStatusTracker(ContextWeakMutablePtr context_);
    ~ExchangeStatusTracker() = default;

    // Register exchange with given segment id and its sink number.
    void registerExchange(const String & query_id, UInt64 exchange_id, size_t parallel_size);
    // Register single exchange status with sink parallel index in an exchange.
    void registerExchangeStatus(const String & query_id, UInt64 exchange_id, UInt64 parallel_index, const ExchangeStatus & exchange_status);
    // Unregister exchagnes for specified query.
    void unregisterExchanges(const String & query_id);
    /// check if query is still alive, used by worker GC
    /// NOTE! it might happen that when server restarts, a new query with the same query_id is created
    /// So far, this is okay for our usage case in GC, because this new query will eventually finish, and all files in worker will be removed.
    bool checkQueryAlive(const String & query_id);
    // Get the most locality-friendly addresses for specified partition range.
    std::vector<AddressInfo>
    getExchangeDataAddrs(PlanSegment * plan_segment, UInt64 start_parallel_index, UInt64 end_parallel_index, double locality_fraction);
    size_t getExchangeDataSize(const String & query_id, UInt64 exchange_id) const;
    const ExchangeStatuses & getExchangeStatusesRef(const String & query_id, UInt64 exchange_id) const;

private:
    // Unregister one exchange, this will delete related status data.
    void unregisterExchange(const String & query_id, UInt64 exchange_id);

    // Protect concurrent access to `exchange_statuses` and `query_to_exchanges`.
    std::mutex exchange_status_mutex;
    std::unordered_map<const ExchangeKey, ExchangeStatuses, ExchangeKey::Hash> exchange_statuses;
    // Store all exchange ids for query, used to delete exchange statuses for a query.
    std::unordered_map<String, std::unordered_set<UInt64>> query_exchange_ids;
    LoggerPtr log = getLogger("ExchangeStatusTracker");
};

using ExchangeStatusTrackerPtr = std::shared_ptr<ExchangeStatusTracker>;
}
