#pragma once

#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Columns/ListIndex.h>
#include <Interpreters/BitMapIndexHelper.h>

class MergeTreeRangeReader;

namespace DB
{

using BitMapIndexInfoPtr = std::shared_ptr<BitMapIndexInfo>;
class MergeTreeBitMapIndexReader
{
public:
    MergeTreeBitMapIndexReader(const String & path, const BitMapIndexInfoPtr & bitmap_index_info, const MergeTreeIndexGranularity & index_granularity, const MarkRanges & mark_ranges);

    virtual String getName() const { return "BitMapIndexReader"; }
    String getPath() const { return path; }

    virtual size_t read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res) = 0;
    bool validIndexReader() { return valid_reader; }
    void initIndexes(const NameSet indexes_)
    {
        output_indexes = std::move(indexes_);
        if (valid_reader)
            getIndexes();
    }

    const NameSet & getIndexNames() const { return index_name_set; }

    const NameSet & getOutputColumnNames() const { return output_indexes; }

    virtual ~MergeTreeBitMapIndexReader();

    virtual BitmapIndexPtr getIndex(const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg);

    virtual void getIndexes();

protected:
    template <typename BITMAP_TYPE>
    void getBaseIndex(std::vector<BitmapIndexPtr> & res_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader);

    const String path;
    const BitMapIndexInfoPtr bitmap_index_info;
    const MergeTreeIndexGranularity & index_granularity;
    const MarkRanges mark_ranges;
    NameSet output_indexes;
    // index-name : bitmap reader
    std::map<String, std::shared_ptr<BitmapIndexReader>> bitmap_index_readers;
    bool valid_reader = true;
    size_t rows_start = 0;
    std::map<String, BitmapIndexPtr> indexes;
    NameSet index_name_set;

};

template <typename BITMAP_TYPE>
void MergeTreeBitMapIndexReader::getBaseIndex(std::vector<BitmapIndexPtr> & res_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader)
{
    if constexpr (std::is_same<BITMAP_TYPE, String>::value)
    {
        const auto string_ptr = static_cast<const ColumnString *>(column.get());
        if (!string_ptr)
            return;

        for (size_t idx = 0; idx < string_ptr->size(); ++idx)
        {
            const String & vid = string_ptr->getDataAt(idx).toString();
            BitmapIndexPtr temp_index = std::make_shared<IListIndex>();
            bool found_vid = bitmap_index_reader->deserialize(vid, *temp_index);
            list_index->setOriginalRows(temp_index->getOriginalRows());
            if (found_vid)
                res_indexes.push_back(temp_index);
        }
    }
    else
    {
        const auto number_ptr = static_cast<const ColumnVector<BITMAP_TYPE> *>(column.get());
        if (!number_ptr)
            return;

        for (size_t idx = 0; idx < number_ptr->size(); ++idx)
        {
            const BITMAP_TYPE & vid = number_ptr->getElement(idx);
            BitmapIndexPtr temp_index = std::make_shared<IListIndex>();
            bool found_vid = bitmap_index_reader->deserialize(vid, *temp_index);
            list_index->setOriginalRows(temp_index->getOriginalRows());
            if (found_vid)
                res_indexes.push_back(temp_index);
        }
    }
}

}
