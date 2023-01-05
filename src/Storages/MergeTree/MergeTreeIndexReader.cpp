#include <Storages/MergeTree/MergeTreeIndexReader.h>


namespace DB
{

MergeTreeIndexReader::MergeTreeIndexReader(
    MergeTreeIndexPtr index_,
    MergeTreeData::DataPartPtr part_,
    size_t marks_count_,
    const MarkRanges & all_mark_ranges_,
    MergeTreeReaderSettings settings)
    : index(index_)
    , stream(
          part_->volume->getDisk(),
          part_->getFullRelativePath() + index->getFileName(),
          index->getFileName(),
          ".idx",
          marks_count_,
          all_mark_ranges_,
          std::move(settings),
          nullptr,
          nullptr,
          &part_->index_granularity_info,
          ReadBufferFromFileBase::ProfileCallback{},
          CLOCK_MONOTONIC_COARSE,
          part_->getFileOffsetOrZero(index->getFileName() + ".idx"),
          part_->getFileSizeOrZero(index->getFileName() + ".idx"),
          part_->getFileOffsetOrZero(index->getFileName() + part_->getMarksFileExtension()),
          part_->getFileSizeOrZero(index->getFileName() + part_->getMarksFileExtension()))
{
    stream.seekToStart();
}

void MergeTreeIndexReader::seek(size_t mark)
{
    stream.seekToMark(mark);
}

MergeTreeIndexGranulePtr MergeTreeIndexReader::read()
{
    auto granule = index->createIndexGranule();
    granule->deserializeBinary(*stream.data_buffer);
    return granule;
}

}
