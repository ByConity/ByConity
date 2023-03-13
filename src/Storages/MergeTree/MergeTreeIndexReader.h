#pragma once

#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeReaderStream.h>

namespace DB
{

/// Abstract class for MergeTree index readers
class IMergeTreeIndexReader
{
public:
    virtual ~IMergeTreeIndexReader() = default;

    virtual void seek(size_t mark) = 0;

    virtual MergeTreeIndexGranulePtr read() = 0;
};

/// Reader for (real) index with physical file `index_name.idx`
class MergeTreeIndexReader : public IMergeTreeIndexReader
{
static constexpr char const * INDEX_FILE_EXTENSION = ".idx";
public:
    MergeTreeIndexReader(
        MergeTreeIndexPtr index_,
        MergeTreeData::DataPartPtr part_,
        size_t marks_count_,
        const MarkRanges & all_mark_ranges_,
        MergeTreeReaderSettings settings,
        MarkCache * mark_cache);

    void seek(size_t mark) override;

    MergeTreeIndexGranulePtr read() override;

private:
    MergeTreeIndexPtr index;
    std::unique_ptr<IMergeTreeReaderStream> stream;
};

/// Reader for virtual index. Virtual index has no physical file
/// We will materialize the index on the fly in `read` method:
/// (1) Read source column of the index --> output some required source columns to calculate the index expression
/// (2) Calculate the index expression --> ouput a column
/// (3) Build the index granule w.r.t. the index type
class MergeTreeVirtualIndexReader : public IMergeTreeIndexReader
{

};
