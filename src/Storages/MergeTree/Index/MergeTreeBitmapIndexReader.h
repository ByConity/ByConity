#pragma once

#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Core/NamesAndTypes.h>
#include <Columns/ListIndex.h>
#include <Columns/SegmentListIndex.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Core/NamesAndTypes.h>

class MergeTreeRangeReader;

namespace DB
{

using BitmapIndexInfoPtr = std::shared_ptr<BitmapIndexInfo>;
class MergeTreeBitmapIndexReader : public MergeTreeColumnIndexReader
{
public:
    MergeTreeBitmapIndexReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info, const MergeTreeIndexGranularity & index_granularity, const MarkRanges & mark_ranges);

    MergeTreeBitmapIndexReader(
        const IMergeTreeDataPartPtr & part,
        const BitmapIndexInfoPtr & bitmap_index_info,
        const MergeTreeIndexGranularity & index_granularity,
        const size_t & segment_granularity,
        const size_t & serializing_granularity,
        const MarkRanges & mark_ranges);

    virtual String getName() const override { return "BitMapIndexReader"; }

    size_t read(size_t  /*from_mark*/, bool  /*continue_reading*/, size_t  /*max_rows_to_read*/, Columns &  /*res*/) override { throw Exception("Not implement", ErrorCodes::NOT_IMPLEMENTED); }
    bool validIndexReader() const override { return valid_reader; }

    void initIndexes(const NameSet & indexes_) override;

    void addSegmentIndexesFromMarkRanges(const MarkRanges & mark_ranges_inc) override;

    const NameSet & getIndexNames() const { return index_name_set; }

    const NameOrderedSet & getOutputColumnNames() const { return output_indexes; }

    const NamesAndTypesList & getOutputColumns() const { return columns; }

    virtual ~MergeTreeBitmapIndexReader() override;

    virtual BitmapIndexPtr getIndex(const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg);

    virtual BitmapIndexPtr getIndex(const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const SetPtr & set_arg);

    virtual BitmapIndexPtr getIndex(const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const SetPtr & set_arg, const MarkRanges & mark_ranges_inc);

    virtual void getIndexes();
    virtual void getIndexesFromMarkRanges(const MarkRanges & mark_ranges_inc);

protected:
    template <typename BitmapType>
    bool getBaseIndex(std::vector<BitmapIndexPtr> & res_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader);
    template <typename BitmapType>
    bool getBaseIndex(std::vector<BitmapIndexPtr> & res_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_readers);
    template <typename BitmapType>
    bool getBaseIndex(std::vector<BitmapIndexPtr> & res_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const MarkRanges & mark_ranges_inc);

    const BitmapIndexInfoPtr bitmap_index_info;
    const MergeTreeIndexGranularity & index_granularity;

    const static size_t DEFAULT_SEGMENT_GRANULARITY;
    const static size_t DEFAULT_SERIALIZING_GRANULARITY;
    const size_t segment_granularity;
    const size_t serializing_granularity;
    MarkRanges mark_ranges;
    
    NameOrderedSet output_indexes;
    // index-name : bitmap reader
    std::map<String, std::shared_ptr<BitmapIndexReader>> bitmap_index_readers;
    std::map<String, std::shared_ptr<SegmentBitmapIndexReader>> segment_bitmap_index_readers{};
    bool valid_reader = true;
    size_t rows_start = 0;
    std::map<String, BitmapIndexPtr> indexes;
    NameSet index_name_set;
    NamesAndTypesList columns;
};

template <typename BitmapType>
bool MergeTreeBitmapIndexReader::getBaseIndex(std::vector<BitmapIndexPtr> & res_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader)
{
    // using container to deserialize vids in one pass
    std::unordered_set<BitmapType> vids;

    if constexpr (std::is_same<BitmapType, String>::value)
    {
        const auto * const string_ptr = static_cast<const ColumnString *>(column.get());
        if (!string_ptr)
            return false;

        for (size_t idx = 0; idx < string_ptr->size(); ++idx)
            vids.emplace(std::move(string_ptr->getDataAt(idx).toString()));
    }
    else
    {
        const auto number_ptr = static_cast<const ColumnVector<BitmapType> *>(column.get());
        if (!number_ptr)
            return false;

        for (size_t idx = 0; idx < number_ptr->size(); ++idx)
            vids.emplace(number_ptr->getElement(idx));
    }

    return bitmap_index_reader->deserializeVids(vids, res_indexes, list_index);
}

template <typename BitmapType>
bool MergeTreeBitmapIndexReader::getBaseIndex(std::vector<BitmapIndexPtr> & res_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader)
{
    std::unordered_set<BitmapType> vids;
    if constexpr (std::is_same<BitmapType, String>::value)
    {
        const auto * string_ptr = static_cast<const ColumnString *>(column.get());
        if (!string_ptr)
            return false;

        for (size_t idx = 0; idx < string_ptr->size(); ++idx)
            vids.emplace(std::move(string_ptr->getDataAt(idx).toString()));
    }
    else
    {
        const auto number_ptr = static_cast<const ColumnVector<BitmapType> *>(column.get());
        if (!number_ptr)
            return false;

        for (size_t idx = 0; idx < number_ptr->size(); ++idx)
            vids.emplace(number_ptr->getElement(idx));
    }

    return segment_bitmap_index_reader->deserializeVids(vids, res_indexes, list_index, mark_ranges);
}

template <typename BitmapType>
bool MergeTreeBitmapIndexReader::getBaseIndex(std::vector<BitmapIndexPtr> & res_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const MarkRanges &mark_ranges_inc)
{
    std::unordered_set<BitmapType> vids;
    if constexpr (std::is_same<BitmapType, String>::value)
    {
        const auto * const string_ptr = static_cast<const ColumnString *>(column.get());
        if (!string_ptr)
            return false;

        for (size_t idx = 0; idx < string_ptr->size(); ++idx)
            vids.emplace(std::move(string_ptr->getDataAt(idx).toString()));
    }
    else
    {
        const auto number_ptr = static_cast<const ColumnVector<BitmapType> *>(column.get());
        if (!number_ptr)
            return false;

        for (size_t idx = 0; idx < number_ptr->size(); ++idx)
            vids.emplace(number_ptr->getElement(idx));
    }

    return segment_bitmap_index_reader->deserializeVids(vids, res_indexes, list_index, mark_ranges_inc);
}

}
