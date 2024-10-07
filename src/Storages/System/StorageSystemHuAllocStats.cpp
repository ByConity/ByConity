#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/System/StorageSystemHuAllocStats.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Pipe.h>
#include <Core/NamesAndTypes.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <Common/formatReadable.h>
#include <fmt/core.h>

#include "config.h"

#if USE_HUALLOC
#    include <hualloc/hu_alloc.h>
#endif


namespace DB
{
StorageSystemHuAllocStats::StorageSystemHuAllocStats(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    ColumnsDescription desc;
    auto columns = getNamesAndTypes();
    for (const auto & col : columns)
    {
        ColumnDescription col_desc(col.name, col.type);
        desc.add(col_desc);
    }
    storage_metadata.setColumns(desc);
    setInMemoryMetadata(storage_metadata);
}

NamesAndTypesList StorageSystemHuAllocStats::getNamesAndTypes()
{
    return {
        { "GiantAlloc",          std::make_shared<DataTypeUInt64>() },
        { "LargeReclaim",       std::make_shared<DataTypeUInt64>() },
        { "SegmentReclaim",     std::make_shared<DataTypeUInt64>() },
        { "LargeCached",        std::make_shared<DataTypeString>() },
        { "SegmentCached",      std::make_shared<DataTypeString>() },
        { "LargeAllocate",      std::make_shared<DataTypeUInt64>() },
        { "LargeFree",          std::make_shared<DataTypeUInt64>() },
        { "SegmentAllocate",      std::make_shared<DataTypeUInt64>() },
        { "SegmentFree",          std::make_shared<DataTypeUInt64>() },
        { "GiantAllocate",      std::make_shared<DataTypeUInt64>() },
        { "GiantFree",          std::make_shared<DataTypeUInt64>() },
    };
}

Pipe StorageSystemHuAllocStats::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    storage_snapshot->check(column_names);

    auto header = storage_snapshot->getMetadataForQuery()->getSampleBlockWithVirtuals(getVirtuals());
    MutableColumns res_columns = header.cloneEmptyColumns();

#if USE_HUALLOC
        size_t col_num = 0;
        res_columns.at(col_num++)->insert(HugeAlloc());
        res_columns.at(col_num++)->insert(LargeReclaimed());
        res_columns.at(col_num++)->insert(SegmentReclaimed());
        res_columns.at(col_num++)->insert(formatReadableSizeWithBinarySuffix(LargeCached()));
        res_columns.at(col_num++)->insert(formatReadableSizeWithBinarySuffix(SegmentCached()));
        res_columns.at(col_num++)->insert(GetTotalLargeAlloc());
        res_columns.at(col_num++)->insert(GetTotalLargeFree());
        res_columns.at(col_num++)->insert(GetTotalSegmentAlloc());
        res_columns.at(col_num++)->insert(GetTotalSegmentFree());
        res_columns.at(col_num++)->insert(GetTotalGiantAlloc());
        res_columns.at(col_num++)->insert(GetTotalGiantFree());
#else
    LOG_INFO(getLogger("StorageSystemHuAllocStats"), "HuAlloc is not enabled");
#endif // USE_HUALLOC

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}

}
