#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <bits/types/clockid_t.h>
#include "IO/ReadBufferFromFileBase.h"
#include "Storages/MergeTree/IMergeTreeReaderStream.h"

namespace DB
{

class MergeTreeDataPartCNCH;
using DataPartCNCHPtr = std::shared_ptr<const MergeTreeDataPartCNCH>;

/// Reader for Wide parts.
class MergeTreeReaderCNCH : public IMergeTreeReader
{
public:
    MergeTreeReaderCNCH(
        const DataPartCNCHPtr & data_part_,
        const NamesAndTypesList & columns_,
        const StorageMetadataPtr & metadata_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        MergeTreeBitMapIndexReader * bitmap_index_reader_,
        const ValueSizeMap & avg_value_size_hints_ = {},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read,
        Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

private:
    using MergeTreeReaderStreamUniquePtr = std::unique_ptr<IMergeTreeReaderStream>;
    using FileStreams = std::map<std::string, MergeTreeReaderStreamUniquePtr>;
    using FileStreamBuilders = std::map<std::string, std::function<MergeTreeReaderStreamUniquePtr()>>;

    void initializeStreams(const ReadBufferFromFileBase::ProfileCallback& profile_callback,
        clockid_t clock_type);
    void initializeStreamForColumnIfNoBurden(const NameAndTypePair& column,
        const ReadBufferFromFileBase::ProfileCallback& profile_callback,
        clockid_t clock_type, FileStreamBuilders* stream_builders);
    void executeFileStreamBuilders(FileStreamBuilders& stream_builders);

    void addStreamsIfNoBurden(const NameAndTypePair& name_and_type,
        const std::function<String(const String&, const ISerialization::SubstreamPath&)>& file_name_getter,
        const ReadBufferFromFileBase::ProfileCallback& profile_callback,
        clockid_t clock_type, FileStreamBuilders* stream_builders);

    size_t next_row_number_to_read = 0;

    IDiskCacheStrategyPtr segment_cache_strategy;
    IDiskCachePtr segment_cache;

    Poco::Logger * log;
};

}
