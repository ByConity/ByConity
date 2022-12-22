#pragma once

#include <Core/Names.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Interpreters/StorageID.h>
#include <Storages/MergeTree/BackgroundProcessList.h>
#include <WorkerTasks/ManipulationType.h>

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

    StorageID storage_id;
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

    explicit ManipulationInfo(StorageID storage_id_);
    void update(const ManipulationInfo & info);
};

using ManipulationInfoPtr = std::shared_ptr<ManipulationInfo>;

struct ManipulationListElement : boost::noncopyable
{
    ManipulationType type;

    std::string task_id;
    std::string related_node;
    std::atomic<time_t> last_touch_time;

    StorageID storage_id;
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

    ManipulationListElement(const ManipulationTaskParams & params, bool disable_memory_tracker);

    ~ManipulationListElement();

    ManipulationInfo getInfo() const;
};

using ManipulationListEntry = BackgroundProcessListEntry<ManipulationListElement, ManipulationInfo>;

class ManipulationList final : public BackgroundProcessList<ManipulationListElement, ManipulationInfo>
{
private:
    using Parent = BackgroundProcessList<ManipulationListElement, ManipulationInfo>;

public:
    using Entry = ManipulationListEntry;

    ManipulationList()
        : Parent(CurrentMetrics::Manipulation)
    {}
};

}
