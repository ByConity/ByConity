#include "IMergeTreeReaderStream.h"
#include <common/range.h>

namespace DB
{

size_t IMergeTreeReaderStream::getRightOffset(size_t right_mark)
{
    /// NOTE: if we are reading the whole file, then right_mark == marks_count
    /// and we will use max_read_buffer_size for buffer size, thus avoiding the need to load marks.

    /// Special case, can happen in Collapsing/Replacing engines
    if (marks_count == 0)
        return 0;

    assert(right_mark <= marks_count);

    if (0 < right_mark && right_mark < marks_count)
    {
        /// Find the right border of the last mark we need to read.
        /// To do that let's find the upper bound of the offset of the last
        /// included mark.

        if (is_low_cardinality_dictionary)
        {

            /// In LowCardinality dictionary several consecutive marks can point to the same offset.
            ///
            /// Also, in some cases, when one granule is not-atomically written (which is possible at merges)
            /// one granule may require reading of two dictionaries which starts from different marks.
            /// The only correct way is to take offset from at least next different granule from the right one.
            /// So, that's why we have to read one extra granule to the right,
            /// while reading dictionary of LowCardinality.
            ///
            /// Example:
            /// Mark 0, points to [0, 8]
            /// Mark 1, points to [0, 8]
            /// Mark 2, points to [0, 8]
            /// Mark 3, points to [0, 8]
            /// Mark 4, points to [42336, 2255]
            /// Mark 5, points to [42336, 2255]  <--- for example need to read until 5
            /// Mark 6, points to [42336, 2255]  <--- not suitable, because have same offset
            /// Mark 7, points to [84995, 7738]  <--- next different mark
            /// Mark 8, points to [84995, 7738]
            /// Mark 9, points to [126531, 8637] <--- what we are looking for

            auto indices = collections::range(right_mark, marks_count);
            auto next_different_mark = [&](auto lhs, auto rhs)
            {
                return marks_loader.getMark(lhs).asTuple() < marks_loader.getMark(rhs).asTuple();
            };
            auto it = std::upper_bound(indices.begin(), indices.end(), right_mark, std::move(next_different_mark));

            if (it == indices.end())
                return file_size;

            right_mark = *it;
        }

        /// This is a good scenario. The compressed block is finished within the right mark,
        /// and previous mark was different.
        if (marks_loader.getMark(right_mark).offset_in_decompressed_block == 0
            && marks_loader.getMark(right_mark) != marks_loader.getMark(right_mark - 1))
            return marks_loader.getMark(right_mark).offset_in_compressed_file;
        /// mark: [offset_in_compressed_file, offset_in_decompressed_block]
        /// If right_mark has non-zero offset in decompressed block, we have to
        /// read its compressed block in a whole, because it may consist of data from previous granule.
        ///
        /// For example:
        /// Mark 6, points to [42336, 2255]
        /// Mark 7, points to [84995, 7738]  <--- right_mark
        /// Mark 8, points to [84995, 7738]
        /// Mark 9, points to [126531, 8637] <--- what we are looking for
        ///
        /// Since mark 7 starts from offset in decompressed block 7738,
        /// it has some data from mark 6 and we have to read
        /// compressed block  [84995; 126531 in a whole.

        auto indices = collections::range(right_mark, marks_count);
        auto next_different_compressed_offset = [&](auto lhs, auto rhs)
        {
            return marks_loader.getMark(lhs).offset_in_compressed_file < marks_loader.getMark(rhs).offset_in_compressed_file;
        };
        auto it = std::upper_bound(indices.begin(), indices.end(), right_mark, std::move(next_different_compressed_offset));

        if (it != indices.end())
            return marks_loader.getMark(*it).offset_in_compressed_file;
    }
    else if (right_mark == 0)
        return marks_loader.getMark(right_mark).offset_in_compressed_file;

    return file_size;
}

void IMergeTreeReaderStream::adjustRightMark(size_t right_mark) {
    auto right_offset = getRightOffset(right_mark);
    if (!right_offset)
    {
        if (last_right_offset && *last_right_offset == 0)
            return;

        last_right_offset = 0; // Zero value means the end of file.
        data_buffer->setReadUntilEnd();
    }
    else
    {
        if (last_right_offset && right_offset <= last_right_offset.value())
            return;

        last_right_offset = right_offset;
        data_buffer->setReadUntilPosition(right_offset);
    }
}

ReadBuffer * IMergeTreeReaderStream::getDataBuffer() {
    return data_buffer;
}

}
