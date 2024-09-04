#include <Storages/MergeTree/LateMaterialize/Stream.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <common/range.h>

namespace DB
{
namespace LateMaterialize
{
    GranuledStream::GranuledStream::GranuledStream(size_t from_mark, size_t to_mark, IMergeTreeReader * merge_tree_reader_)
        : current_mark(from_mark)
        , last_mark(to_mark)
        , merge_tree_reader(merge_tree_reader_)
        , index_granularity(&(merge_tree_reader->data_part->index_granularity))
    {
    }

    std::pair<size_t,size_t> GranuledStream::read(Columns & columns)
    {
        size_t num_requested_rows = index_granularity->getMarkRows(current_mark);
        size_t num_rows = merge_tree_reader->readRows(current_mark, 0, num_requested_rows,
            last_mark, nullptr, columns);
        /// if (0 < num_rows && num_rows < num_requested_rows) /// Can happen if this is the last mark
        if (0 < num_rows && num_rows < num_requested_rows) /// Can happen if this is the last mark
            num_requested_rows = num_rows;
        ++current_mark;
        return {num_requested_rows, num_rows};
    }

    void GranuledStream::skip()
    {
        current_mark++;
    }

    bool GranuledStream::isFinished() const
    {
        return current_mark == last_mark;
    }

    size_t GranuledStream::position() const
    {
        return index_granularity->getMarkStartingRow(current_mark);
    }

    size_t GranuledStream::rowsInCurrentMark() const
    {
        return index_granularity->getMarkRows(current_mark);
    }

}
}
