#include <Storages/MergeTree/Index/MergeTreeBitmapIndexMultipleReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Common/escapeForFileName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

MergeTreeBitmapIndexMultipleReader::MergeTreeBitmapIndexMultipleReader(
    const IMergeTreeDataPartPtr & part_,
    const BitmapIndexInfoPtr & bitmap_index_info_,
    const MergeTreeIndexGranularity & index_granularity_,
    const MarkRanges & mark_ranges_)
    : MergeTreeBitmapIndexSingleReader(part_, bitmap_index_info_, index_granularity_, mark_ranges_)
{
}

MergeTreeBitmapIndexMultipleReader::MergeTreeBitmapIndexMultipleReader(
    const IMergeTreeDataPartPtr & part_,
    const BitmapIndexInfoPtr & bitmap_index_info_,
    const MergeTreeIndexGranularity & index_granularity_,
    const size_t & segment_granularity_,
    const size_t & serializing_granularity_,
    const MarkRanges & mark_ranges_)
    : MergeTreeBitmapIndexSingleReader(part_, bitmap_index_info_, index_granularity_, segment_granularity_, serializing_granularity_, mark_ranges_)
{
}

/** main logic of bitmap index read */
size_t MergeTreeBitmapIndexMultipleReader::read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res)
{
    size_t prev_rows_start = 0;
    size_t prev_res_num = 0;
    size_t offset = 0;
    
    for (const auto & output_name : output_indexes)
    {
        prev_rows_start = rows_start;

        auto set_it = bitmap_index_info->set_args.find(output_name);
        if (set_it == bitmap_index_info->set_args.end())
            throw Exception("Cannot find set for column " + output_name, ErrorCodes::LOGICAL_ERROR);
        const auto & set_ptr = set_it->second.front();
        if (!set_ptr)
            throw Exception("Cannot find set ptr for column " + set_it->first, ErrorCodes::LOGICAL_ERROR);
        
        const DataTypePtr & data_type = set_ptr->getDataTypes().at(0);

        auto result_type = std::make_shared<DataTypeArray>(data_type);

        if (!res[offset])
            res[offset] = result_type->createColumn();
        MutableColumnPtr column = res[offset++]->assumeMutable();

        if (!continue_reading)
            prev_rows_start = index_granularity.getMarkStartingRow(from_mark);

        auto result_it = element_indexes.find(output_name);
        if (result_it == element_indexes.end())
            throw Exception("Cannot find element bitmap indexes for column " + output_name, ErrorCodes::LOGICAL_ERROR);

        if (!result_it->second.begin()->second)
            throw Exception("Find nullptr element bitmap indexes for " + result_it->first, ErrorCodes::LOGICAL_ERROR);
        
        size_t rows_end = std::min(prev_rows_start + max_rows_to_read, result_it->second.begin()->second->getOriginalRows());
       
        auto & set_indexes = result_it->second;

        WhichDataType to_type(*data_type);

        if (to_type.isUInt8())
            readImpl<UInt8>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isUInt16())
            readImpl<UInt16>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isUInt32())
            readImpl<UInt32>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isUInt64())
            readImpl<UInt64>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isInt8())
            readImpl<Int8>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isInt16())
            readImpl<Int16>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isInt32())
            readImpl<Int32>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isInt64())
            readImpl<Int64>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isFloat32())
            readImpl<Float32>(column, prev_rows_start, rows_end, set_indexes);
        else if (to_type.isFloat64())
            readImpl<Float64>(column, prev_rows_start, rows_end, set_indexes);
        else
            throw Exception("Data type is not support in arraySetGet", ErrorCodes::NOT_IMPLEMENTED);

        size_t res_num = rows_end - prev_rows_start;
        prev_rows_start = rows_end;
        if (prev_res_num == 0)
            prev_res_num = res_num;
        else if (prev_res_num != res_num)
            throw Exception("Mismatch rows number of " + output_name, ErrorCodes::LOGICAL_ERROR);
    }

    rows_start = prev_rows_start;

    return prev_res_num;
}
}
