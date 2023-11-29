#pragma once

#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>

class MergeTreeRangeReader;
/**
 * This class implement arraySetGet by using bitmap index.
 * It gets index for each elements in set of arraySetGet.
 * If BitmapIndexReturnType is SINGLE, it returns the first element if the bitmap row is true
 * If BitmapIndexReturnType is MULTIPLE, it returns the all elements that the bitmap row is true.
 * 
 * for example, arraySetGetAny(ab_version, (1,2)) will return 1 if bitmap 1 is true in this row.
 * arraySetGet(ab_version, (1,2)) will return [1,2] if bitmap 1,2 are true in this row.
 * 
 * getIndexes() will init element_indexes, which is a map<result_column_name, vector<element_index>>
 * 
 * 
 **/
namespace DB
{
class MergeTreeBitmapIndexSingleReader : public MergeTreeBitmapIndexReader
{
public:
    MergeTreeBitmapIndexSingleReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info, const MergeTreeIndexGranularity & index_granularity, const MarkRanges & mark_ranges);
    MergeTreeBitmapIndexSingleReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info, const MergeTreeIndexGranularity & index_granularity, const size_t & segment_granularity, const size_t & serializing_granularity, const MarkRanges & mark_ranges);

    String getName() const override { return "BitmapIndexSingleReader"; }

    /* Return the result of index-OR, and materialize it as ColumnUInt8*/
    size_t read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res) override;

    void getIndexes() override;

protected:

    /**
     * key: result_column_name:
     * value:
     *      key: set element value
     *      vector: set element index
     **/
    std::map<String, std::map<Field, BitmapIndexPtr>> element_indexes;

    template <typename BitmapType>
    void getBaseIndex(std::map<Field, BitmapIndexPtr> & res_element_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader);
    template <typename BitmapType>
    void getBaseIndex(std::map<Field, BitmapIndexPtr> & res_element_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader);

    std::map<Field, BitmapIndexPtr> getElementsIndex(const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg);
    std::map<Field, BitmapIndexPtr> getElementsIndex(const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const SetPtr & set_arg);

private:

    template <typename BitmapType>
    void readImpl
    (
        MutableColumnPtr & column,
        size_t prev_rows_start,
        size_t rows_end,
        std::map<Field, BitmapIndexPtr> & set_indexes
    );
};

template <typename BitmapType>
void MergeTreeBitmapIndexSingleReader::readImpl
(
    MutableColumnPtr & column,
    size_t prev_rows_start,
    size_t rows_end,
    std::map<Field, BitmapIndexPtr> & set_indexes
)
{
    if constexpr (std::is_same<BitmapType, String>::value)
    {
        throw Exception(" not implement ", ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        auto * res_nullable = typeid_cast<ColumnNullable *>(column.get());
        
        if (!res_nullable)
            throw Exception("SINGLE bitmap index computation should return nullable column", ErrorCodes::LOGICAL_ERROR);

        auto & res_vec = typeid_cast<ColumnVector<BitmapType> &>(res_nullable->getNestedColumn()).getData();
        auto & res_null_map = res_nullable->getNullMapData();
        size_t initial_size = res_vec.size();

        if (rows_end > prev_rows_start)
        {
            res_vec.resize_fill(initial_size + rows_end - prev_rows_start);
            res_null_map.resize_fill(initial_size + rows_end - prev_rows_start, 1);
        }

        for (auto it = set_indexes.begin(); it != set_indexes.end(); ++it)
        {
            const auto & bitmap = it->second->getIndex();
            Roaring::const_iterator begin = bitmap.begin();
            begin.equalorlarger(prev_rows_start);

            for (; begin != bitmap.end(); begin++)
            {
                if (*begin >= rows_end)
                    break;
                
                if (res_null_map[initial_size + *begin - prev_rows_start])
                {
                    res_vec[initial_size + *begin - prev_rows_start] = it->first.get<BitmapType>();
                    res_null_map[initial_size + *begin - prev_rows_start] = 0;
                }
            }
        }
    }
}

template <typename BitmapType>
void MergeTreeBitmapIndexSingleReader::getBaseIndex(std::map<Field, BitmapIndexPtr> & res_element_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader)
{
    if constexpr (std::is_same<BitmapType, String>::value)
    {
        const auto *const string_ptr = static_cast<const ColumnString *>(column.get());
        if (!string_ptr)
            return;

        for (size_t idx = 0; idx < string_ptr->size(); ++idx)
        {
            const String & vid = string_ptr->getDataAt(idx).toString();
            BitmapIndexPtr temp_index = std::make_shared<IListIndex>();
            bitmap_index_reader->deserialize(vid, *temp_index);
            list_index->setOriginalRows(temp_index->getOriginalRows());
            
            res_element_indexes.emplace((*string_ptr)[idx], temp_index);
        }
    }
    else
    {
        const auto number_ptr = static_cast<const ColumnVector<BitmapType> *>(column.get());
        if (!number_ptr)
            return;

        for (size_t idx = 0; idx < number_ptr->size(); ++idx)
        {
            const BitmapType & vid = number_ptr->getElement(idx);
            BitmapIndexPtr temp_index = std::make_shared<IListIndex>();
            bitmap_index_reader->deserialize(vid, *temp_index);
            list_index->setOriginalRows(temp_index->getOriginalRows());
            
            res_element_indexes.emplace((*number_ptr)[idx], temp_index);
        }
    }
}

template <typename BitmapType>
void MergeTreeBitmapIndexSingleReader::getBaseIndex(std::map<Field, BitmapIndexPtr> & res_element_indexes, BitmapIndexPtr & list_index, const ColumnPtr & column, const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader)
{
    if constexpr (std::is_same<BitmapType, String>::value)
    {
        const auto *const string_ptr = static_cast<const ColumnString *>(column.get());
        if (!string_ptr)
            return;

        for (size_t idx = 0; idx < string_ptr->size(); ++idx)
        {
            const String & vid = string_ptr->getDataAt(idx).toString();
            BitmapIndexPtr temp_index = std::make_shared<IListIndex>();
            segment_bitmap_index_reader->deserialize(vid, *temp_index, mark_ranges);
            list_index->setOriginalRows(temp_index->getOriginalRows());
            
            res_element_indexes.emplace((*string_ptr)[idx], temp_index);
        }
    }
    else
    {
        const auto number_ptr = static_cast<const ColumnVector<BitmapType> *>(column.get());
        if (!number_ptr)
            return;

        for (size_t idx = 0; idx < number_ptr->size(); ++idx)
        {
            const BitmapType & vid = number_ptr->getElement(idx);
            BitmapIndexPtr temp_index = std::make_shared<IListIndex>();
            segment_bitmap_index_reader->deserialize(vid, *temp_index, mark_ranges);
            list_index->setOriginalRows(temp_index->getOriginalRows());
            
            res_element_indexes.emplace((*number_ptr)[idx], temp_index);
        }
    }
}

}
