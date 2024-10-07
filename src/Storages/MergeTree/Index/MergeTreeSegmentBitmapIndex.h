#pragma once

#include <Common/Logger.h>
#include <sstream>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{


class MergeTreeSegmentBitmapIndex
{
public:
    explicit MergeTreeSegmentBitmapIndex(MergeTreeData & data);

    static bool isSegmentBitmapIndexColumn(const DataTypePtr & column_type);
    static bool isSegmentBitmapIndexColumn(const IDataType & type);
    bool isSegmentBitmapIndexColumn(const String & name);

    static bool isValidSegmentBitmapIndexColumnType(DataTypePtr column_type);
    static bool isValidSegmentBitmapIndexType(const WhichDataType & type);
    static void checkSegmentBitmapStorageGranularity(const ColumnsDescription & all_columns, const MergeTreeSettingsPtr & settings);

    static bool needBuildSegmentIndex(const String & part_path, const String & column_name);

private:
    static inline String getBitDirExtension() { return SEGMENT_BITMAP_DIRECTORY_EXTENSION; }
    static inline String getBitTabExtension() { return SEGMENT_BITMAP_TABLE_EXTENSION; }
    static inline String getBitIdxExtension() { return SEGMENT_BITMAP_IDX_EXTENSION; }

    MergeTreeData & data;
    LoggerPtr log;
};
}
