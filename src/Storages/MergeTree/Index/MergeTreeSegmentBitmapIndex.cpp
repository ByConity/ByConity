#include <Storages/MergeTree/Index/MergeTreeSegmentBitmapIndex.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeScheduler.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
inline bool MergeTreeSegmentBitmapIndex::isValidSegmentBitmapIndexType(const WhichDataType & type)
{
    return (type.isNativeInt() || type.isNativeUInt() || type.isFloat() || type.isString());
}

bool MergeTreeSegmentBitmapIndex::isValidSegmentBitmapIndexColumnType(DataTypePtr column_type)
{
    WhichDataType data_type(column_type);
    if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(column_type.get()))
    {
        const auto& nested_type = array->getNestedType();
        if (const DataTypeNullable * nullable = typeid_cast<const DataTypeNullable *>(nested_type.get()))
            return isValidSegmentBitmapIndexType(WhichDataType(nullable->getNestedType()));

        return isValidSegmentBitmapIndexType(WhichDataType(nested_type));
    }
    else 
    {
        if (const DataTypeNullable * nullable = typeid_cast<const DataTypeNullable *>(column_type.get()))
            return isValidSegmentBitmapIndexType(WhichDataType(nullable->getNestedType()));

        return isValidSegmentBitmapIndexType(data_type);
    }
}

bool MergeTreeSegmentBitmapIndex::isSegmentBitmapIndexColumn(const DataTypePtr & column_type)
{
    return column_type->isSegmentBitmapIndex();
}

bool MergeTreeSegmentBitmapIndex::isSegmentBitmapIndexColumn(const IDataType & type)
{
    return type.isSegmentBitmapIndex();
}

bool MergeTreeSegmentBitmapIndex::isSegmentBitmapIndexColumn(const String & name)
{
    return MergeTreeSegmentBitmapIndex::isSegmentBitmapIndexColumn(data.getInMemoryMetadataPtr()->getColumns().get(name).type);
}

bool MergeTreeSegmentBitmapIndex::needBuildSegmentIndex(const String & part_path, const String & column_name)
{
    Poco::File seg_idx_file(part_path + column_name + SEGMENT_BITMAP_IDX_EXTENSION);
    Poco::File seg_tab_file(part_path + column_name + SEGMENT_BITMAP_TABLE_EXTENSION);
    Poco::File seg_dir_file(part_path + column_name + SEGMENT_BITMAP_DIRECTORY_EXTENSION);
    return !(seg_idx_file.exists() && seg_tab_file.exists() && seg_dir_file.exists());
}

void MergeTreeSegmentBitmapIndex::checkSegmentBitmapStorageGranularity(const ColumnsDescription & all_columns, const MergeTreeSettingsPtr & settings) 
{
    auto all_physical_columns = all_columns.getAllPhysical();
    bool has_segment_bitmap_column = std::any_of(all_physical_columns.begin(), all_physical_columns.end(),
            [](const auto & item) { return MergeTreeSegmentBitmapIndex::isSegmentBitmapIndexColumn(item.type); }
    );

    if (!has_segment_bitmap_column)
        return;

    if (settings->bitmap_index_segment_granularity < settings->index_granularity)
        throw Exception("SegmentBitmap granularity fault: segment_granularity should be greater than or equal to index_granularity: ", ErrorCodes::LOGICAL_ERROR);

    if (settings->bitmap_index_serializing_granularity < settings->bitmap_index_segment_granularity)
        throw Exception("SegmentBitmap granularity fault: serializing_granularity should be greater than or equal to segment_granularity: ", ErrorCodes::LOGICAL_ERROR);
}

}
