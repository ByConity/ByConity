#pragma once

#include <Common/Logger.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <common/logger_useful.h>

namespace DB
{
class MergeTreeBitmapIndex
{

public:
    explicit MergeTreeBitmapIndex(MergeTreeData & data);

    // static bool isSegmentBitmapIndexColumn(const DataTypePtr & column_type);
    // static bool isSegmentBitmapIndexColumn(const IDataType & type);
    // bool isSegmentBitmapIndexColumn(const String & name);

    static bool isBitmapIndexColumn(const DataTypePtr & column_type);
    static bool isBitmapIndexColumn(const IDataType & type);
    bool isBitmapIndexColumn(const String & name);

    static bool isValidBitmapIndexColumnType(DataTypePtr column_type);
    template <bool with_bitmap_type = false>
    static bool isValidBitmapIndexType(const WhichDataType & type);
    static void checkValidBitmapIndexType(const ColumnsDescription & all_columns);

    static bool needBuildIndex(const String & part_path, const String & column_name);
    // static bool needBuildSegmentIndex(const String & part_path, const String & column_name);

private:

    MergeTreeData & data;
    LoggerPtr log;
};

}
