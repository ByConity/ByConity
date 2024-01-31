
#pragma once

#include <Columns/IColumn.h>
#include <Storages/MergeTree/LateMaterialize/ReadResult.h>


namespace DB
{
class IMergeTreeReader;
class MergeTreeIndexGranularity;
namespace LateMaterialize
{
    // Read 1 granules at a time and can skip granule
    struct GranuledStream
    {
        size_t current_mark = -1;
        size_t last_mark = -1;
        IMergeTreeReader * merge_tree_reader = nullptr;
        const MergeTreeIndexGranularity * index_granularity = nullptr;

        GranuledStream(size_t from_mark, size_t to_mark, IMergeTreeReader * merge_tree_reader_);
        GranuledStream() = default;

        bool isFinished() const;

        // Skip current granules
        void skip();

        // Get current position of mark (in rows metric)
        size_t position() const;

        // Number of rows we're about to read in next `read()` call
        size_t rowsInCurrentMark() const;

        // Read current granules and move to next mark
        std::pair<size_t, size_t> read(Columns & columns);
    };

}
}

