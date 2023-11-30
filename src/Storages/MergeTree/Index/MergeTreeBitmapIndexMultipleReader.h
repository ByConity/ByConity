#pragma once

#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexSingleReader.h>
#include <Columns/ColumnArray.h>

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

class MergeTreeBitmapIndexMultipleReader : public MergeTreeBitmapIndexSingleReader
{
public:

    MergeTreeBitmapIndexMultipleReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info,
                                       const MergeTreeIndexGranularity & index_granularity, const MarkRanges & mark_ranges);
    MergeTreeBitmapIndexMultipleReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info, const MergeTreeIndexGranularity & index_granularity, const size_t & segment_granularity, const size_t & serializing_granularity, const MarkRanges & mark_ranges);

    String getName() const override { return "BitmapIndexMultipleReader"; }

    /* Return the result of index-OR, and materialize it as ColumnUInt8*/
    size_t read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res) override;

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
void MergeTreeBitmapIndexMultipleReader::readImpl
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
        auto * res_array = typeid_cast<ColumnArray *>(column.get());
        
        if (!res_array)
            throw Exception("SINGLE bitmap index computation should return Array column", ErrorCodes::LOGICAL_ERROR);

        auto & res_offsets = typeid_cast<ColumnArray::ColumnOffsets &>(res_array->getOffsetsColumn()).getData();

        std::vector<Array> temp_res_vec;
        temp_res_vec.resize(rows_end - prev_rows_start);

        for (auto & set_index : set_indexes)
        {
            const auto & bitmap = set_index.second->getIndex();
            Roaring::const_iterator begin = bitmap.begin();
            begin.equalorlarger(prev_rows_start);

            for (; begin != bitmap.end(); begin++)
            {
                if (*begin >= rows_end)
                    break;
                
                temp_res_vec[*begin - prev_rows_start].push_back(set_index.first.get<BitmapType>());
            }
        }

        for (size_t i = 0; i < rows_end - prev_rows_start; ++i)
        {
            size_t size = temp_res_vec[i].size();
            for (size_t j = 0; j < size; ++j)
                res_array->getData().insert(temp_res_vec[i][j]);
            res_offsets.push_back(res_offsets.back() + size);
        }

    }
}

}
