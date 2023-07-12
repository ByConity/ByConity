#pragma once

#include <Core/NamesAndTypes.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>

#include <future>
#include <mutex>

namespace DB
{
class Context;
class MergeTreeMetaBase;

/**
 *  An prefetcher for MergeTree data part on remote storage during merging:
 *
 *  Once submitted a part to this prefetcher, it try to split the whole data file
 *  into some segments according to the offsets of stream (i.e. file in checksums)
 *  So that prefetcher can download those segments asynchronously later. Then
 *  following MERGE stages, it calculates which stage does each stream belong to.
 *
 *  A reader (MergeTreePrefetchedReaderCNCH) could find the future segment by
 *  <mvcc version, stream name>, and trigger prefetching next stage. And reader
 *  could release the used segments so that prefetcher could remove temporary files
 *  ahead.
 *
 *  Mapping:
 *  (a stream is a file in checksums)
 *  column -> streams  {e.g. column.bin column.mrk}
 *  stream -> segment -> stage
 *  segment contains { continuous streams }
 *  stage -> list{ segment }
 */
class CnchMergePrefetcher
{
public:
    struct FutureSegment
    {
        CnchMergePrefetcher * prefetcher{nullptr};
        const IMergeTreeDataPart * part{nullptr};
        int future_access{0};
        int stage{-1};

        ReservationPtr reservation;
        // Segment relative path to disk root path, including table paths, etc
        String data_relative_path;
        off_t offset{0};
        size_t size{0};
        std::shared_future<void> done;

        auto valid() const { return done.valid(); }

        std::tuple<DiskPtr, String, off_t> get() const
        {
            done.get();
            return {reservation->getDisk(), data_relative_path, offset};
        }
    };
    using FutureSegments = std::vector<FutureSegment>;

    struct PartFutureFiles : private boost::noncopyable
    {
        PartFutureFiles(CnchMergePrefetcher & p_, String n_) : prefetcher(p_), part_name(std::move(n_)) { }

        CnchMergePrefetcher & prefetcher;
        String part_name; /// For logging
        String fixed_injected_column;
        std::unordered_map<String, std::pair<Int64, size_t>> stream_to_mutation_index;
        std::unordered_map<Int64, FutureSegments> mutation_segments;
        std::vector<std::vector<FutureSegment *>> prefetch_stages;

        auto & getFixedInjectedColumn() const { return fixed_injected_column; }

        void tryPrefetchStage(int stage);
        void schedulePrefetchTask(FutureSegment & future_segment);

        FutureSegment * tryGetFutureSegment(const String & stream_name);
        FutureSegment * getFutureSegmentAndPrefetch(const String & stream_name);
        void releaseSegment(const String & stream_name);
    };

    void submitDataPart(
        const IMergeTreeDataPartPtr & data_part, const NamesAndTypesList & merging_columns, const NamesAndTypesList & gathering_columns);

    PartFutureFiles * tryGetFutureFiles(const String & part_name)
    {
        if (auto it = part_to_future_files.find(part_name); it != part_to_future_files.end())
            return it->second.get();
        else
            return nullptr;
    }

    CnchMergePrefetcher(const Context & context_, const MergeTreeMetaBase & storage_, const String & temp_dir_);

    ~CnchMergePrefetcher();


private:
    const MergeTreeMetaBase & storage;
    size_t segment_size;
    String temp_dir_rel_path;

    std::unordered_map<String, std::unique_ptr<PartFutureFiles>> part_to_future_files;
    std::set<String> columns_may_inject;

    std::atomic_int cancel{0};
};

} /// EOF of DB
