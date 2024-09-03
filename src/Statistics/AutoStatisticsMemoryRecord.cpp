#include <atomic>
#include <shared_mutex>
#include <Interpreters/Context.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/AutoStatisticsMemoryRecord.h>
namespace DB::Statistics::AutoStats
{

struct AutoStatisticsMemoryRecord::DataImpl
{
    std::shared_mutex mu;
    std::unordered_map<TableKey, std::atomic<UInt64>> table_udi;
};

AutoStatisticsMemoryRecord::~AutoStatisticsMemoryRecord() noexcept
{
}

AutoStatisticsMemoryRecord::AutoStatisticsMemoryRecord() : impl(std::make_unique<DataImpl>())
{
}

AutoStatisticsMemoryRecord & AutoStatisticsMemoryRecord::instance()
{
    static AutoStatisticsMemoryRecord record;
    return record;
}

void AutoStatisticsMemoryRecord::addUdiCount(const TableKey & table_key, UInt64 count)
{
    std::shared_lock shared_lock(impl->mu);
    if (auto iter = impl->table_udi.find(table_key); iter != impl->table_udi.end())
    {
        iter->second += count;
    }
    else
    {
        shared_lock.unlock();
        std::unique_lock lck(impl->mu);
        impl->table_udi[table_key] += count;
    }
}

auto AutoStatisticsMemoryRecord::getAll() -> std::map<TableKey, UInt64>
{
    std::shared_lock lck(impl->mu);
    std::map<TableKey, UInt64> result;
    for (auto & [k, v] : impl->table_udi)
    {
        result.emplace(k, v.load());
    }

    return result;
}

auto AutoStatisticsMemoryRecord::getAndClearAll() -> std::map<TableKey, UInt64>
{
    std::unique_lock lck(impl->mu);
    decltype(impl->table_udi) tmp_map;
    tmp_map.swap(impl->table_udi);
    lck.unlock();

    std::map<TableKey, UInt64> result;
    for (auto & [k, v] : tmp_map)
    {
        result.emplace(k, v.load());
    }

    return result;
}

void ModifiedCounter::analyze(const StoragePtr & storage, const MutableMergeTreeDataPartsCNCHVector & parts)
{
    if (parts.empty())
    {
        return;
    }


    if (!storage || storage->is_detached || storage->is_dropped)
    {
        return;
    }

    auto table_options = CatalogAdaptor::getTableOptionsForStorage(*storage);

    if (!table_options.is_auto_updatable)
    {
        return;        
    }

    auto uuid = storage->getStorageUUID();
    for (const auto & part : parts)
    {
        counter[uuid] += part->rows_count;
    }
}
void ModifiedCounter::merge(const ModifiedCounter & right)
{
    for (const auto & [uuid, count] : right.counter)
        this->counter[uuid] += count;
}

void AutoStatisticsMemoryRecord::append(const ModifiedCounter & counter)
{
    if (!AutoStatisticsManager::xmlConfigIsEnable())
        return;

    for (const auto & [uuid, count] : counter.counter)
    {
        // usually there is just one
        this->addUdiCount(uuid, count);
    }
}

void AutoStatisticsMemoryRecord::append(const std::unordered_map<UUID, UInt64> & counter)
{
    if (!AutoStatisticsManager::xmlConfigIsEnable())
        return;

    for (const auto & [uuid, count] : counter)
    {
        this->addUdiCount(uuid, count);
    }
}

}
