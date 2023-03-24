#pragma once

#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeReaderStream.h>

namespace DB
{
class IMergeTreeIndexReader;
class MergeTreeIndexReader;
class MergeTreeHypoIndexReader;
using MergeTreeIndexReadePtr = std::unique_ptr<IMergeTreeIndexReader>;

/// Base class for index readers
class IMergeTreeIndexReader
{
public:
    explicit IMergeTreeIndexReader(MergeTreeIndexPtr index_)
        : index(index_)
    {
    }
    virtual ~IMergeTreeIndexReader() = default;

    virtual void seek(size_t mark) = 0;

    virtual MergeTreeIndexGranulePtr read() = 0;

    template<typename ...Args>
    static MergeTreeIndexReadePtr create(MergeTreeIndexPtr index, Args &&... args)
    {
        if (index->isHypothetical())
            return std::make_unique<MergeTreeHypoIndexReader>(index, std::forward<Args>(args)...);
        else
            return std::make_unique<MergeTreeIndexReader>(index, std::forward<Args>(args)...);
    }
protected:
    MergeTreeIndexPtr index;
};


/// Class for reading `real` index, expects index file to be present
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
    std::unique_ptr<IMergeTreeReaderStream> stream;
};

/// Class for reading `hypothetical` index, expects index file to be absent
/// The index file will be materialized on the fly from column files
class MergeTreeHypoIndexReader : public IMergeTreeIndexReader
{
public:
    MergeTreeHypoIndexReader(
        MergeTreeIndexPtr index_,
        MergeTreeData::DataPartPtr part_,
        size_t marks_count_,
        const MarkRanges & all_mark_ranges_,
        MergeTreeReaderSettings settings,
        MarkCache * mark_cache);

    void seek(size_t mark) override;

    MergeTreeIndexGranulePtr read() override;
private:

};

}
