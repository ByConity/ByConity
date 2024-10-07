#include <Storages/MergeTree/Index/MergeTreeBitmapIndex.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/MergeTree/MergeScheduler.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>

#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

MergeTreeBitmapIndex::MergeTreeBitmapIndex(MergeTreeData & data_)
    : data(data_)
    , log(getLogger("MergeTreeBitmapIndex"))
{

}

// bool MergeTreeBitmapIndex::isSegmentBitmapIndexColumn(const DataTypePtr & column_type)
// {
//     return column_type->isSegmentBitmapIndex();
// }

// bool MergeTreeBitmapIndex::isSegmentBitmapIndexColumn(const IDataType & type)
// {
//     return type.isSegmentBitmapIndex();
// }

// bool MergeTreeBitmapIndex::isSegmentBitmapIndexColumn(const String & name)
// {
//     return MergeTreeBitmapIndex::isSegmentBitmapIndexColumn(data.getInMemoryMetadataPtr()->getColumns().get(name).type);
// }

bool MergeTreeBitmapIndex::isBitmapIndexColumn(const String & name)
{
    return MergeTreeBitmapIndex::isBitmapIndexColumn(data.getInMemoryMetadataPtr()->getColumns().get(name).type);
}

bool MergeTreeBitmapIndex::isBitmapIndexColumn(const DataTypePtr & column_type)
{
    return column_type->isBitmapIndex();
}

bool MergeTreeBitmapIndex::isBitmapIndexColumn(const IDataType & type)
{
    return type.isBitmapIndex();
}

bool MergeTreeBitmapIndex::needBuildIndex(const String & part_path, const String & column_name)
{
    Poco::File adx_file(part_path + column_name + BITMAP_IDX_EXTENSION);
    Poco::File irk_file(part_path + column_name + BITMAP_IRK_EXTENSION);
    return !(adx_file.exists() && irk_file.exists());
}

// bool MergeTreeBitmapIndex::needBuildSegmentIndex(const String & part_path, const String & column_name)
// {
//     Poco::File seg_idx_file(part_path + column_name + SEGMENT_BITMAP_IDX_EXTENSION);
//     Poco::File seg_tab_file(part_path + column_name + SEGMENT_BITMAP_TABLE_EXTENSION);
//     Poco::File seg_dir_file(part_path + column_name + SEGMENT_BITMAP_DIRECTORY_EXTENSION);
//     return !(seg_idx_file.exists() && seg_tab_file.exists() && seg_dir_file.exists());
// }

template <bool with_bitmap_type>
inline bool MergeTreeBitmapIndex::isValidBitmapIndexType(const WhichDataType & type)
{
    if constexpr (with_bitmap_type)
        return (type.isNativeInt() || type.isNativeUInt() || type.isFloat() || type.isString()) /* || type.isBitmap32() */ || type.isBitmap64();
    else
        return (type.isNativeInt() || type.isNativeUInt() || type.isFloat() || type.isString());
}

// bool MergeTreeBitmapIndex::isValidBitmapIndexColumnType(DataTypePtr column_type)
// {
//     WhichDataType data_type(column_type);
//     if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(column_type.get()))
//     {
//         const auto& nested_type = array->getNestedType();
//         if (const DataTypeNullable * nullable = typeid_cast<const DataTypeNullable *>(nested_type.get()))
//             return isValidBitmapIndexType(WhichDataType(nullable->getNestedType()));

//         return isValidBitmapIndexType(WhichDataType(nested_type));
//     }
//     else 
//     {
//         if (const DataTypeNullable * nullable = typeid_cast<const DataTypeNullable *>(column_type.get()))
//             return isValidBitmapIndexType(WhichDataType(nullable->getNestedType()));

//         return isValidBitmapIndexType<true>(data_type);
//     }
// }

/// TODO: Only support Array type now
bool MergeTreeBitmapIndex::isValidBitmapIndexColumnType(DataTypePtr column_type)
{
    WhichDataType data_type(column_type);
    if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(column_type.get()))
    {
        const auto& nested_type = array->getNestedType();
        return isValidBitmapIndexType(WhichDataType(nested_type));
    }
        
    return false;
}

void MergeTreeBitmapIndex::checkValidBitmapIndexType(const ColumnsDescription & all_columns)
{
    for (const auto & column : all_columns)
    {
        if (!MergeTreeBitmapIndex::isBitmapIndexColumn(column.type))
            continue;

        if (!MergeTreeBitmapIndex::isValidBitmapIndexColumnType(column.type))
            throw Exception("Unsupported type of bitmap index: " + column.type->getName() + ". Need: Array(String/Int/UInt/Float)", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}

}
