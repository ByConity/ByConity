#pragma once

#include <IO/WriteBufferFromFile.h>
#include <DataTypes/IDataType.h>
#include <IO/HashingWriteBuffer.h>
#include <Columns/ListIndex.h>
#include <Columns/SegmentListIndex.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

namespace DB
{

struct IndexParams
{
    // these parameters are used by both bitmap and segment bitmap index
    bool enable_build_index = false;
    bool enable_index_in_alter = false;
    bool enable_run_optimization = false;
    size_t max_parallel_threads = 16;
    size_t index_granularity = 8192;
    size_t segment_granularity = 65536;
    size_t serializing_granularity = 65536;

    IndexParams() = default;

    IndexParams(const bool enable_build_index_,
                const bool enable_build_index_in_alter_,
                const bool enable_run_optimization_,
                const size_t max_parallel_threads_,
                const size_t index_granularity_)
        :enable_build_index(enable_build_index_),
         enable_index_in_alter(enable_build_index_in_alter_),
         enable_run_optimization(enable_run_optimization_),
         max_parallel_threads(max_parallel_threads_),
         index_granularity(index_granularity_){}

    IndexParams(const bool enable_build_index_,
                const bool enable_build_index_in_alter_,
                const bool enable_run_optimization_,
                const size_t max_parallel_threads_,
                const size_t index_granularity_,
                const size_t segment_granularity_,
                const size_t serializing_granularity_)
        :enable_build_index(enable_build_index_),
         enable_index_in_alter(enable_build_index_in_alter_),
         enable_run_optimization(enable_run_optimization_),
         max_parallel_threads(max_parallel_threads_),
         index_granularity(index_granularity_), 
         segment_granularity(segment_granularity_),
         serializing_granularity(serializing_granularity_){}
};

struct ColumnBitmapIndex
{
    ColumnBitmapIndex(const String & escaped_column_name_,
                      const String & data_path_,
                      const IDataType & type_,
                      const IndexParams & bitmap_params = IndexParams());

    void finalize() const;
    void sync() const;
    void addToChecksums(MergeTreeDataPartChecksums & checksums) const;
    void createBitmapIndex(const String & escaped_column_name, const String & data_path, const IDataType & type);

    String escaped_column_name;

    // register bitmap indexes here for this column
    using BitmapColumnListIndexesPtr = std::unique_ptr<IBitmapColumnListIndexes>;
    BitmapColumnListIndexesPtr bitmap_index = nullptr;
    IndexParams bitmap_params;
    bool only_write_bitmap_index = false;
};

struct ColumnSegmentBitmapIndex
{
    ColumnSegmentBitmapIndex(const String & escaped_column_name_,
                      const String & data_path_,
                      const IDataType & type_,
                      const IndexParams & bitmap_params = IndexParams());

    void finalize() const;
    void sync() const;
    void addToChecksums(MergeTreeDataPartChecksums & checksums) const;
    void createSegmentBitmapIndex(const String & escaped_column_name, const String & data_path, const IDataType & type);

    String escaped_column_name;

    // register bitmap indexes here for this column
    using SegmentBitmapColumnListIndexesPtr = std::unique_ptr<ISegmentBitmapColumnListIndexes>;
    SegmentBitmapColumnListIndexesPtr bitmap_index = nullptr;
    IndexParams bitmap_params;
    bool only_write_bitmap_index = false;
};

}
