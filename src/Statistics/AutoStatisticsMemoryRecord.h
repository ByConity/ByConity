#pragma once

#include <Statistics/CatalogAdaptor.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>

namespace DB::Statistics::AutoStats
{
class ModifiedCounter;
class AutoStatisticsMemoryRecord : boost::noncopyable
{
public:
    AutoStatisticsMemoryRecord();
    // this is thread safe
    using TableKey = UUID;
    void addUdiCount(const TableKey & table, UInt64 count);
    // simplify adder of part
    void append(const ModifiedCounter & counter);
    void append(const std::unordered_map<UUID, UInt64> & counter);

    std::map<TableKey, UInt64> getAndClearAll();
    std::map<TableKey, UInt64> getAll();


    static AutoStatisticsMemoryRecord & instance();
    ~AutoStatisticsMemoryRecord() noexcept;

private:
    struct DataImpl;
    std::unique_ptr<DataImpl> impl;
};

class ModifiedCounter
{
public:
    friend class AutoStatisticsMemoryRecord;
    void analyze(const StoragePtr & storage, const MutableMergeTreeDataPartsCNCHVector & parts);
    void merge(const ModifiedCounter & right);
    bool empty() const { return counter.empty(); }

private:
    std::unordered_map<UUID, UInt64> counter;
};

} // DB::Statistics
