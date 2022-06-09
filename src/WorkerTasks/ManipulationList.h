#pragma once

#include <Core/Names.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <WorkerTasks/ManipulationType.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <boost/noncopyable.hpp>

namespace CurrentMetrics
{
extern const Metric Manipulation;
}

namespace DB
{
struct ManipulationTaskParams;
class ManipulationTask;
class ManipulationList;

struct ManipulationInfo
{
    ManipulationType type;

    std::string task_id;
    std::string related_node;

    std::string database;
    std::string table;
    UUID uuid;
    Float64 elapsed;
    Float64 progress;
    UInt64 num_parts;
    Array result_part_names;
    Array source_part_names;
    std::string partition_id;
    UInt64 total_size_bytes_compressed;
    UInt64 total_size_marks;
    UInt64 total_rows_count;
    UInt64 bytes_read_uncompressed;
    UInt64 bytes_written_uncompressed;
    UInt64 rows_read;
    UInt64 rows_written;
    UInt64 columns_written;
    UInt64 memory_usage;
    UInt64 thread_id;

    void update(const ManipulationInfo & info);
};

using ManipulationInfoPtr = std::shared_ptr<ManipulationInfo>;

struct ManipulationListElement : boost::noncopyable
{
    ManipulationType type;

    std::string task_id;
    std::string related_node;
    std::atomic<time_t> last_touch_time;

    std::string database;
    std::string table;
    UUID uuid;
    std::string partition_id;

    Names result_part_names;
    Int64 result_data_version{};

    UInt64 num_parts{};
    Names source_part_names;
    Int64 source_data_version{};

    Stopwatch watch;
    std::atomic<Float64> progress{};
    std::atomic<bool> is_cancelled{};

    UInt64 total_size_bytes_compressed{};
    UInt64 total_size_marks{};
    UInt64 total_rows_count{};
    std::atomic<UInt64> bytes_read_uncompressed{};
    std::atomic<UInt64> bytes_written_uncompressed{};

    /// In case of Vertical algorithm they are actual only for primary key columns
    std::atomic<UInt64> rows_read{};
    std::atomic<UInt64> rows_written{};

    /// Updated only for Vertical algorithm
    std::atomic<UInt64> columns_written{};

    MemoryTracker memory_tracker{VariableContext::Process};
    MemoryTracker * background_thread_memory_tracker = nullptr;
    MemoryTracker * background_thread_memory_tracker_prev_parent = nullptr;

    /// Poco thread number used in logs
    UInt64 thread_id;

    ManipulationListElement(const ManipulationTaskParams & params, bool disable_memory_tracker = false);

    ~ManipulationListElement();

    ManipulationInfo getInfo() const;
};

class ManipulationList;

class ManipulationListEntry
{
    ManipulationList * list;
    using container_t = std::list<ManipulationListElement>;
    container_t::iterator it;

    CurrentMetrics::Increment inc{CurrentMetrics::Manipulation};

    ManipulationListEntry(const ManipulationListEntry &) = delete;
    ManipulationListEntry & operator=(const ManipulationListEntry &) = delete;

public:
    ManipulationListEntry(ManipulationListEntry && rhs) { *this = std::move(rhs); }
    ManipulationListEntry & operator=(ManipulationListEntry && rhs)
    {
        list = rhs.list;
        it = rhs.it;
        rhs.list = nullptr;
        return *this;
    }

    ManipulationListEntry(ManipulationList & l, const container_t::iterator i) : list(&l), it(i) { }
    ~ManipulationListEntry();

    ManipulationListElement * operator->() { return &*it; }
    const ManipulationListElement * operator->() const { return &*it; }

    ManipulationListElement * get() { return &*it; }
    const ManipulationListElement * get() const { return &*it; }
};

class ManipulationList
{
    friend class ManipulationListEntry;
    using container_t = std::list<ManipulationListElement>;
    using info_container_t = std::vector<ManipulationInfo>;

    mutable std::mutex mutex;
    container_t container;

public:
    using Entry = ManipulationListEntry;

    template <typename... Args>
    Entry insert(Args &&... args)
    {
        std::lock_guard lock{mutex};
        return Entry(*this, container.emplace(container.end(), std::forward<Args>(args)...));
    }

    size_t size() const
    {
        std::lock_guard lock(mutex);
        return container.size();
    }

    info_container_t get() const
    {
        std::lock_guard lock{mutex};
        info_container_t res;
        for (const auto & elem : container)
            res.emplace_back(elem.getInfo());
        return res;
    }

    template <class F>
    void apply(F && f)
    {
        std::lock_guard lock(mutex);
        f(container);
    }
};

inline ManipulationListEntry::~ManipulationListEntry()
{
    if (list)
    {
        std::lock_guard lock(list->mutex);
        list->container.erase(it);
    }
}

}
