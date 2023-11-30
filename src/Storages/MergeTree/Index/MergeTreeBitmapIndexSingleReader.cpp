#include <Storages/MergeTree/Index/MergeTreeBitmapIndexSingleReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

MergeTreeBitmapIndexSingleReader::MergeTreeBitmapIndexSingleReader
(
    const IMergeTreeDataPartPtr & part_,
    const BitmapIndexInfoPtr & bitmap_index_info_,
    const MergeTreeIndexGranularity & index_granularity_,
    const MarkRanges & mark_ranges_
)
: MergeTreeBitmapIndexReader(part_, bitmap_index_info_, index_granularity_, mark_ranges_)
{
}

MergeTreeBitmapIndexSingleReader::MergeTreeBitmapIndexSingleReader
(
    const IMergeTreeDataPartPtr & part_,
    const BitmapIndexInfoPtr & bitmap_index_info_,
    const MergeTreeIndexGranularity & index_granularity_,
    const size_t & segment_granularity_,
    const size_t & serializing_granularity_, 
    const MarkRanges & mark_ranges_
)
: MergeTreeBitmapIndexReader(part_, bitmap_index_info_, index_granularity_, segment_granularity_, serializing_granularity_, mark_ranges_)
{
}

/** main logic of bitmap index read */
size_t MergeTreeBitmapIndexSingleReader::read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res)
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

        auto result_type = std::make_shared<DataTypeNullable>(data_type);

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
            throw Exception("Data type is not support in arraySetGetAny", ErrorCodes::NOT_IMPLEMENTED);

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


/**
 * init element_indexes by traverse bitmap_index_info->index_names, each of them is a result column.
 * Each map element contains only one column that will be checked by bitmap index.
 * We will get its corresponding bitmap index.
 **/
void MergeTreeBitmapIndexSingleReader::getIndexes()
{
    // for each bitmap, load the needed posting list
    for (auto it = bitmap_index_info->index_names.begin(); it != bitmap_index_info->index_names.end(); ++it)
    {
        std::map<Field, BitmapIndexPtr> set_element_index;

        auto set_it = bitmap_index_info->set_args.find(it->first);
        if (set_it == bitmap_index_info->set_args.end())
            throw Exception("Cannot find set for column " + it->first, ErrorCodes::LOGICAL_ERROR);

        if (it->second.size() > 1 || set_it->second.size() > 1)
            throw Exception("Default BitmapIndexReader cannot handle a function with multiple column / sets", ErrorCodes::LOGICAL_ERROR);

        if (it->second.empty() || set_it->second.empty())
            throw Exception("Default BitmapIndexReader cannot handle a function with empty column / sets", ErrorCodes::LOGICAL_ERROR);

        auto index_name = it->second.front();
        auto set_ptr = set_it->second.front();

        // choose which reader to use
#define SETBITMAPINDEXITER(IT, RD) auto IT = RD.find(index_name);
#define SETBITMAPINDEX(IT, RD) if (IT != RD.end())                              \
        {                                                                       \
            set_element_index = getElementsIndex(IT->second, set_ptr);  \
        }

        SETBITMAPINDEXITER(seg_reader_it, segment_bitmap_index_readers)
        SETBITMAPINDEXITER(reader_it, bitmap_index_readers)
        
        SETBITMAPINDEX(seg_reader_it, segment_bitmap_index_readers)
        else SETBITMAPINDEX(reader_it, bitmap_index_readers)
#undef SETBITMAPINDEXITER
#undef SETBITMAPINDEX
        else
        {
            throw Exception("No BitmapIndexReader of " + index_name + " has been found", ErrorCodes::LOGICAL_ERROR);
        }

         // (column name, set element index)
        element_indexes.emplace(it->first, set_element_index);
    }
}

/**
 * for a set, it contains multiple elements, each element has a bitmap index
 * this function get index for each element and return a map to recording each set element and its bitmap 
 **/
std::map<Field, BitmapIndexPtr> MergeTreeBitmapIndexSingleReader::getElementsIndex(const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySetGet ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitmapIndexSingleReader", ErrorCodes::ILLEGAL_COLUMN);

    const ColumnPtr & column_ptr = elements[0];
    std::map<Field, BitmapIndexPtr> res_element_indexes;

    BitmapIndexPtr criterion = std::make_shared<IListIndex>();

    const DataTypes & data_types = set_arg->getDataTypes();
    if (data_types.size() != 1)
        throw Exception("Set contains multiple types", ErrorCodes::LOGICAL_ERROR);

    const IDataType & data_type = *(data_types.at(0));
    WhichDataType to_type(data_type);

    if (to_type.isUInt8())
        getBaseIndex<UInt8>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt16())
        getBaseIndex<UInt16>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt32())
        getBaseIndex<UInt32>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt64())
        getBaseIndex<UInt64>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt8())
        getBaseIndex<Int8>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt16())
        getBaseIndex<Int16>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt32())
        getBaseIndex<Int32>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt64())
        getBaseIndex<Int64>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isFloat32())
        getBaseIndex<Float32>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isFloat64())
        getBaseIndex<Float64>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isString())
        getBaseIndex<String>(res_element_indexes, criterion, column_ptr, bitmap_index_reader);

    return res_element_indexes;
}

/**
 * for a set, it contains multiple elements, each element has a bitmap index
 * this function get index for each element and return a map to recording each set element and its bitmap 
 **/
std::map<Field, BitmapIndexPtr> MergeTreeBitmapIndexSingleReader::getElementsIndex(const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const SetPtr & set_arg)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySetGet ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitmapIndexSingleReader", ErrorCodes::ILLEGAL_COLUMN);

    const ColumnPtr & column_ptr = elements[0];
    std::map<Field, BitmapIndexPtr> res_element_indexes;

    BitmapIndexPtr criterion = std::make_shared<IListIndex>();

    const DataTypes & data_types = set_arg->getDataTypes();
    if (data_types.size() != 1)
        throw Exception("Set contains multiple types", ErrorCodes::LOGICAL_ERROR);

    const IDataType & data_type = *(data_types.at(0));
    WhichDataType to_type(data_type);

    if (to_type.isUInt8())
        getBaseIndex<UInt8>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt16())
        getBaseIndex<UInt16>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt32())
        getBaseIndex<UInt32>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt64())
        getBaseIndex<UInt64>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt8())
        getBaseIndex<Int8>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt16())
        getBaseIndex<Int16>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt32())
        getBaseIndex<Int32>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt64())
        getBaseIndex<Int64>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isFloat32())
        getBaseIndex<Float32>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isFloat64())
        getBaseIndex<Float64>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isString())
        getBaseIndex<String>(res_element_indexes, criterion, column_ptr, segment_bitmap_index_reader);

    return res_element_indexes;
}


}
