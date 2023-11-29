#include <DataTypes/DataTypeBitMap64.h>
#include <Storages/MergeTree/ColumnBitmapIndex.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

ColumnBitmapIndex::ColumnBitmapIndex(
    const String & escaped_column_name_,
    const String & data_path_,
    const IDataType & type_,
    const IndexParams & bitmap_params_)
    : escaped_column_name(escaped_column_name_),
      bitmap_params(bitmap_params_),
      only_write_bitmap_index(bitmap_params.enable_index_in_alter)
{
    if (bitmap_params.enable_build_index)
        createBitmapIndex(escaped_column_name_, data_path_, type_);
}

void ColumnBitmapIndex::createBitmapIndex(const String & column_name, const String & data_path, const IDataType & type)
{
    std::shared_ptr<WhichDataType> bitmap_type = nullptr;
    if (const auto * data_type = typeid_cast<const DataTypeArray *>(&type))
    {
        const auto & inner_array_type = data_type->getNestedType();
        if (const auto * inner_type = typeid_cast<const DataTypeNullable *>(inner_array_type.get()))
            bitmap_type = std::make_shared<WhichDataType>(*(inner_type->getNestedType()));
        else
            bitmap_type = std::make_shared<WhichDataType>(*inner_array_type);
    }
    else if (const auto * inner_type = typeid_cast<const DataTypeNullable *>(&type))
    {
        bitmap_type = std::make_shared<WhichDataType>(*(inner_type->getNestedType()));
    }
    else if (typeid_cast<const DataTypeUInt8 *>(&type) || typeid_cast<const DataTypeUInt16 *>(&type)
             || typeid_cast<const DataTypeUInt32 *>(&type) || typeid_cast<const DataTypeUInt64 *>(&type)
             || typeid_cast<const DataTypeInt8 *>(&type) || typeid_cast<const DataTypeInt16 *>(&type)
             || typeid_cast<const DataTypeInt32 *>(&type) || typeid_cast<const DataTypeInt64 *>(&type)
             || typeid_cast<const DataTypeFloat32 *>(&type) || typeid_cast<const DataTypeFloat64 *>(&type)
             || typeid_cast<const DataTypeString *>(&type) 
             || typeid_cast<const DataTypeBitMap64 *>(&type) /* || typeid_cast<const DataTypeBitMap32 * >(&type) */)
        bitmap_type = std::make_shared<WhichDataType>(type);
    else
        return;

    WhichDataType & to_type = *bitmap_type;

    if (to_type.isUInt8())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<UInt8>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isUInt16())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<UInt16>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isUInt32())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<UInt32>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isUInt64())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<UInt64>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isInt8())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<Int8>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isInt16())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<Int16>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isInt32())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<Int32>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isInt64())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<Int64>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isFloat32())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<Float32>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isFloat64())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<Float64>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isString())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<String>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    else if (to_type.isBitmap64())
        bitmap_index = std::make_unique<BitmapColumnListIndexes<UInt64>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
    // else if (to_type.isBitmap32())
    //     bitmap_index = std::make_unique<BitmapColumnListIndexes<UInt32>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, BitmapIndexMode::ROW);
}

void ColumnBitmapIndex::finalize() const
{
    if (bitmap_index)
        bitmap_index->finalize();
}

void ColumnBitmapIndex::sync() const
{
}


void ColumnBitmapIndex::addToChecksums(MergeTreeDataPartChecksums &checksums) const
{
    if (bitmap_index)
        bitmap_index->addToChecksums(checksums);
}

ColumnSegmentBitmapIndex::ColumnSegmentBitmapIndex(
    const String & escaped_column_name_,
    const String & data_path_,
    const IDataType & type_,
    const IndexParams & bitmap_params_)
    : escaped_column_name(escaped_column_name_),
      bitmap_params(bitmap_params_),
      only_write_bitmap_index(bitmap_params.enable_index_in_alter)
{
    if (bitmap_params.enable_build_index || bitmap_params.enable_index_in_alter)
        createSegmentBitmapIndex(escaped_column_name_, data_path_, type_);
}

void ColumnSegmentBitmapIndex::createSegmentBitmapIndex(const String & column_name, const String & data_path, const IDataType & type)
{
    std::shared_ptr<WhichDataType> bitmap_type = nullptr;
    if (const auto * data_type = typeid_cast<const DataTypeArray *>(&type))
    {
        const auto & inner_array_type = data_type->getNestedType();
        if (const auto * inner_type = typeid_cast<const DataTypeNullable *>(inner_array_type.get()))
            bitmap_type = std::make_shared<WhichDataType>(*(inner_type->getNestedType()));
        else
            bitmap_type = std::make_shared<WhichDataType>(*inner_array_type);
    }
    else if (const auto * inner_type = typeid_cast<const DataTypeNullable *>(&type))
    {
        bitmap_type = std::make_shared<WhichDataType>(*(inner_type->getNestedType()));
    }
    else if (typeid_cast<const DataTypeUInt8 *>(&type) || typeid_cast<const DataTypeUInt16 *>(&type)
             || typeid_cast<const DataTypeUInt32 *>(&type) || typeid_cast<const DataTypeUInt64 *>(&type)
             || typeid_cast<const DataTypeInt8 *>(&type) || typeid_cast<const DataTypeInt16 *>(&type)
             || typeid_cast<const DataTypeInt32 *>(&type) || typeid_cast<const DataTypeInt64 *>(&type)
             || typeid_cast<const DataTypeFloat32 *>(&type) || typeid_cast<const DataTypeFloat64 *>(&type)
             || typeid_cast<const DataTypeString *>(&type))
        bitmap_type = std::make_shared<WhichDataType>(type);
    else
        return;

    WhichDataType & to_type = *bitmap_type;

    if (to_type.isUInt8())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<UInt8>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isUInt16())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<UInt16>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isUInt32())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<UInt32>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isUInt64())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<UInt64>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isInt8())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<Int8>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isInt16())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<Int16>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isInt32())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<Int32>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isInt64())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<Int64>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isFloat32())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<Float32>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isFloat64())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<Float64>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
    else if (to_type.isString())
        bitmap_index = std::make_unique<SegmentBitmapColumnListIndexes<String>>(data_path, column_name, bitmap_params.enable_run_optimization, bitmap_params.max_parallel_threads, bitmap_params.index_granularity, bitmap_params.segment_granularity, bitmap_params.serializing_granularity, BitmapIndexMode::ROW);
}

void ColumnSegmentBitmapIndex::sync() const
{

}

void ColumnSegmentBitmapIndex::addToChecksums(MergeTreeDataPartChecksums & checksums) const
{
    if (bitmap_index)
        bitmap_index->addToChecksums(checksums);
}

void ColumnSegmentBitmapIndex::finalize() const
{
    if (bitmap_index)
        bitmap_index->finalize();
}


}

