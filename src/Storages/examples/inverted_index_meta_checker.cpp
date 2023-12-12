#include <cstddef>
#include <filesystem>
#include <iostream>
#include <vector>
#include <IO/ReadBufferFromFile.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <fmt/core.h>

int main(int argc, char ** argv)
{
    size_t file_size;
    size_t segment_nums;
    std::vector<DB::GinIndexSegment> segments_mata;

    if (argc !=2)
    {
        std::cout << "Use ./inverted_index_meta_checker [file_full_path_for_gin_seg]" << std::endl;
        return 0;
    }

    String file_path(argv[1]);

    file_size = fs::file_size(file_path);
    segment_nums = file_size / sizeof(DB::GinIndexSegment);

    std::cout << fmt::format("segment total size : {}, size per sgement : {}, segment nums {}", 
        file_size, sizeof(DB::GinIndexSegment), segment_nums)  << std::endl;

    if (file_size % sizeof(DB::GinIndexSegment) != 0)
    {
        std::cout << "error size of inverted meta" << std::endl;
        return 0;
    }

    segments_mata.resize(segment_nums);

    DB::ReadBufferFromFile file_buff(file_path);

    file_buff.readStrict(reinterpret_cast<char *>(segments_mata.data()), segment_nums * sizeof(DB::GinIndexSegment));

    for (auto segment : segments_mata)
    {
        std::cout << fmt::format(
            "segment_id: {}, next_row_id: {}, dict_start_offset: {}, postings_start_offset: {}\n",
            segment.segment_id,
            segment.next_row_id,
            segment.dict_start_offset,
            segment.postings_start_offset);
    }

    return 0;
}