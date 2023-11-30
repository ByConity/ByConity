#include <Storages/MergeTree/Index/MergeTreeBitmapIndexIntersectBoolReader.h>
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

MergeTreeBitmapIndexIntersectBoolReader::MergeTreeBitmapIndexIntersectBoolReader
    (
        const IMergeTreeDataPartPtr & part_,
        const BitmapIndexInfoPtr & bitmap_index_info_,
        const MergeTreeIndexGranularity & index_granularity_,
        const MarkRanges & mark_ranges_
    )
    : MergeTreeBitmapIndexBoolReader(part_, bitmap_index_info_, index_granularity_, mark_ranges_)
{
}

MergeTreeBitmapIndexIntersectBoolReader::MergeTreeBitmapIndexIntersectBoolReader
    (
        const IMergeTreeDataPartPtr & part_,
        const BitmapIndexInfoPtr & bitmap_index_info_,
        const MergeTreeIndexGranularity & index_granularity_,
        const size_t & segment_granularity_,
        const size_t & serializing_granularity_, 
        const MarkRanges & mark_ranges_
    )
    : MergeTreeBitmapIndexBoolReader(part_, bitmap_index_info_, index_granularity_, segment_granularity_, serializing_granularity_, mark_ranges_)
{
}

BitmapIndexPtr MergeTreeBitmapIndexIntersectBoolReader::getIndex(const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySet functions ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitmapIndexReader", ErrorCodes::ILLEGAL_COLUMN);

    const ColumnPtr & column_ptr = elements[0];
    std::vector<BitmapIndexPtr> res_indexes;

    BitmapIndexPtr criterion = std::make_shared<IListIndex>();

    const DataTypes & data_types = set_arg->getDataTypes();
    if (data_types.size() != 1)
        throw Exception("Set contains multiple types", ErrorCodes::LOGICAL_ERROR);

    const IDataType & data_type = *(data_types.at(0));
    WhichDataType to_type(data_type);

    bool all_matched = true;
    if (to_type.isUInt8())
        all_matched = getBaseIndex<UInt8>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt16())
        all_matched = getBaseIndex<UInt16>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt32())
        all_matched = getBaseIndex<UInt32>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isUInt64())
        all_matched = getBaseIndex<UInt64>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt8())
        all_matched = getBaseIndex<Int8>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt16())
        all_matched = getBaseIndex<Int16>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt32())
        all_matched = getBaseIndex<Int32>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isInt64())
        all_matched = getBaseIndex<Int64>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isFloat32())
        all_matched = getBaseIndex<Float32>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isFloat64())
        all_matched = getBaseIndex<Float64>(res_indexes, criterion, column_ptr, bitmap_index_reader);
    else if (to_type.isString())
        all_matched = getBaseIndex<String>(res_indexes, criterion, column_ptr, bitmap_index_reader);

    if (!all_matched)
        return criterion;

    auto & bitmap = criterion->getIndex();
    for (size_t idx = 0; idx < res_indexes.size(); ++idx)
    {
        if (idx == 0)
            bitmap |= res_indexes.at(idx)->getIndex();
        else
            bitmap &= res_indexes.at(idx)->getIndex();
    }

    return criterion;
}

BitmapIndexPtr MergeTreeBitmapIndexIntersectBoolReader::getIndex(const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const SetPtr & set_arg)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySet functions ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitmapIndexReader", ErrorCodes::ILLEGAL_COLUMN);

    const ColumnPtr & column_ptr = elements[0];
    std::vector<BitmapIndexPtr> res_indexes;

    BitmapIndexPtr criterion = std::make_shared<IListIndex>();

    const DataTypes & data_types = set_arg->getDataTypes();
    if (data_types.size() != 1)
        throw Exception("Set contains multiple types", ErrorCodes::LOGICAL_ERROR);

    const IDataType & data_type = *(data_types.at(0));
    WhichDataType to_type(data_type);

    bool all_matched = true;
    if (to_type.isUInt8())
        all_matched = getBaseIndex<UInt8>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt16())
        all_matched = getBaseIndex<UInt16>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt32())
        all_matched = getBaseIndex<UInt32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isUInt64())
        all_matched = getBaseIndex<UInt64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt8())
        all_matched = getBaseIndex<Int8>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt16())
        all_matched = getBaseIndex<Int16>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt32())
        all_matched = getBaseIndex<Int32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isInt64())
        all_matched = getBaseIndex<Int64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isFloat32())
        all_matched = getBaseIndex<Float32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isFloat64())
        all_matched = getBaseIndex<Float64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);
    else if (to_type.isString())
        all_matched = getBaseIndex<String>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader);

    if (!all_matched)
        return criterion;

    auto & bitmap = criterion->getIndex();
    for (size_t idx = 0; idx < res_indexes.size(); ++idx)
    {
        if (idx == 0)
            bitmap |= res_indexes.at(idx)->getIndex();
        else
            bitmap &= res_indexes.at(idx)->getIndex();
    }

    return criterion;
}

BitmapIndexPtr MergeTreeBitmapIndexIntersectBoolReader::getIndex(const std::shared_ptr<SegmentBitmapIndexReader> & segment_bitmap_index_reader, const SetPtr & set_arg, const MarkRanges & mark_ranges_inc)
{
    if (!set_arg)
        throw Exception("Illegal arguments of arraySet functions ", ErrorCodes::ILLEGAL_COLUMN);

    const Columns & elements = set_arg->getSetElements();
    if (elements.size() != 1)
        throw Exception("Set size is not correct for MergeTreeBitmapIndexReader", ErrorCodes::ILLEGAL_COLUMN);

    const ColumnPtr & column_ptr = elements[0];
    std::vector<BitmapIndexPtr> res_indexes;

    BitmapIndexPtr criterion = std::make_shared<IListIndex>();

    const DataTypes & data_types = set_arg->getDataTypes();
    if (data_types.size() != 1)
        throw Exception("Set contains multiple types", ErrorCodes::LOGICAL_ERROR);

    const IDataType & data_type = *(data_types.at(0));
    WhichDataType to_type(data_type);

    bool all_matched = true;
    if (to_type.isUInt8())
        all_matched = getBaseIndex<UInt8>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isUInt16())
        all_matched = getBaseIndex<UInt16>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isUInt32())
        all_matched = getBaseIndex<UInt32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isUInt64())
        all_matched = getBaseIndex<UInt64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isInt8())
        all_matched = getBaseIndex<Int8>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isInt16())
        all_matched = getBaseIndex<Int16>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isInt32())
        all_matched = getBaseIndex<Int32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isInt64())
        all_matched = getBaseIndex<Int64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isFloat32())
        all_matched = getBaseIndex<Float32>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isFloat64())
        all_matched = getBaseIndex<Float64>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);
    else if (to_type.isString())
        all_matched = getBaseIndex<String>(res_indexes, criterion, column_ptr, segment_bitmap_index_reader, mark_ranges_inc);

    if (!all_matched)
        return criterion;

    auto & bitmap = criterion->getIndex();
    for (size_t idx = 0; idx < res_indexes.size(); ++idx)
    {
        if (idx == 0)
            bitmap |= res_indexes.at(idx)->getIndex();
        else
            bitmap &= res_indexes.at(idx)->getIndex();
    }

    return criterion;
}

}
