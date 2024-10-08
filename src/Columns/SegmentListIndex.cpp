#include <Columns/ListIndex.h>
#include <Columns/SegmentListIndex.h>

#include <Common/StringUtils/StringUtils.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Common/escapeForFileName.h>

namespace DB
{
SegmentBitmapIndexWriter::SegmentBitmapIndexWriter(String path, String name, BitmapIndexMode bitmap_index_mode_, const bool & enable_run_optimization_)
{
    // get suffix
    bitmap_index_mode = bitmap_index_mode_;
    String seg_dir_suffix = seg_dir_suffix_vec[bitmap_index_mode];
    String seg_tab_suffix = seg_tab_suffix_vec[bitmap_index_mode];
    String seg_idx_suffix = seg_idx_suffix_vec[bitmap_index_mode];

    if (!endsWith(path, "/")) path.append("/");

    enable_run_optimization = enable_run_optimization_;

    String column_name = escapeForFileName(name);

    // create buffer
    if (!seg_dir_suffix.empty())
        seg_dir = std::make_unique<WriteBufferFromFile>(path+column_name+seg_dir_suffix, DBMS_DEFAULT_BUFFER_SIZE,
        O_WRONLY | O_APPEND | O_CREAT);

    seg_idx = std::make_unique<WriteBufferFromFile>(path+column_name+seg_idx_suffix, DBMS_DEFAULT_BUFFER_SIZE,
        O_WRONLY | O_APPEND | O_CREAT);
    hash_seg_idx = std::make_unique<HashingWriteBuffer>(*seg_idx);
    // use default CompressionSettings
    compressed_seg_idx = std::make_unique<CompressedWriteBuffer>(*hash_seg_idx);
    hash_compressed = std::make_unique<HashingWriteBuffer>(*compressed_seg_idx);

    seg_tab = std::make_unique<WriteBufferFromFile>(path+column_name+seg_tab_suffix, DBMS_DEFAULT_BUFFER_SIZE,
        O_WRONLY | O_APPEND | O_CREAT);
    hash_seg_tab = std::make_unique<HashingWriteBuffer>(*seg_tab);
}

SegmentBitmapIndexWriter::SegmentBitmapIndexWriter(String path, BitmapIndexMode bitmap_index_mode_, const bool & enable_run_optimization_)
{
    // get suffix
    bitmap_index_mode = bitmap_index_mode_;
    String seg_dir_suffix = seg_dir_suffix_vec[bitmap_index_mode];
    String seg_tab_suffix = seg_tab_suffix_vec[bitmap_index_mode];
    String seg_idx_suffix = seg_idx_suffix_vec[bitmap_index_mode];

    enable_run_optimization = enable_run_optimization_;

    // create buffer
    if (!seg_dir_suffix.empty())
    {
        seg_dir = std::make_unique<WriteBufferFromFile>(path+seg_dir_suffix, DBMS_DEFAULT_BUFFER_SIZE,
        O_WRONLY | O_APPEND | O_CREAT);
        hash_seg_dir = std::make_unique<HashingWriteBuffer>(*seg_dir);
    }

    seg_idx = std::make_unique<WriteBufferFromFile>(path+seg_idx_suffix, DBMS_DEFAULT_BUFFER_SIZE,
        O_WRONLY | O_APPEND | O_CREAT);
    hash_seg_idx = std::make_unique<HashingWriteBuffer>(*seg_idx);
    // use default CompressionSettings
    compressed_seg_idx = std::make_unique<CompressedWriteBuffer>(*hash_seg_idx);
    hash_compressed = std::make_unique<HashingWriteBuffer>(*compressed_seg_idx);

    seg_tab = std::make_unique<WriteBufferFromFile>(path+seg_tab_suffix, DBMS_DEFAULT_BUFFER_SIZE,
        O_WRONLY | O_APPEND | O_CREAT);
    hash_seg_tab = std::make_unique<HashingWriteBuffer>(*seg_tab);
}

void SegmentBitmapIndexWriter::addToChecksums(MergeTreeData::DataPart::Checksums & checksums, const String & column_name)
{
    String seg_dir_suffix = seg_dir_suffix_vec[bitmap_index_mode];
    String seg_tab_suffix = seg_tab_suffix_vec[bitmap_index_mode];
    String seg_idx_suffix = seg_idx_suffix_vec[bitmap_index_mode];

    checksums.files[column_name + seg_idx_suffix].is_compressed = true;
    checksums.files[column_name + seg_idx_suffix].uncompressed_size = hash_compressed->count();
    checksums.files[column_name + seg_idx_suffix].uncompressed_hash = hash_compressed->getHash();
    checksums.files[column_name + seg_idx_suffix].file_size = hash_seg_idx->count();
    checksums.files[column_name + seg_idx_suffix].file_hash = hash_seg_idx->getHash();

    checksums.files[column_name + seg_tab_suffix].file_size = hash_seg_tab->count();
    checksums.files[column_name + seg_tab_suffix].file_hash = hash_seg_tab->getHash();

    checksums.files[column_name + seg_dir_suffix].file_size = hash_seg_dir->count();
    checksums.files[column_name + seg_dir_suffix].file_hash = hash_seg_dir->getHash();
}

SegmentBitmapIndexReader::SegmentBitmapIndexReader(String path_, String name, BitmapIndexMode bitmap_index_mode_, const MergeTreeIndexGranularity & index_granularity_, size_t segment_granularity_, size_t serializing_granularity_) :
    path(path_), column_name(name), bitmap_index_mode(bitmap_index_mode_), index_granularity(index_granularity_), segment_granularity(segment_granularity_), serializing_granularity(serializing_granularity_)
{
    if (!endsWith(path, "/")) path.append("/");
    column_name = escapeForFileName(name);
    init();
}

void SegmentBitmapIndexReader::init()
{
    String seg_dir_suffix = seg_dir_suffix_vec[bitmap_index_mode];
    String seg_tab_suffix = seg_tab_suffix_vec[bitmap_index_mode];
    String seg_idx_suffix = seg_idx_suffix_vec[bitmap_index_mode];
    try
    {
        compressed_idx = std::make_unique<CompressedReadBufferFromFile>(path+column_name+seg_idx_suffix, 0, 0, 0, nullptr);
        seg_tab = std::make_unique<ReadBufferFromFile>(path+column_name+seg_tab_suffix, DBMS_DEFAULT_BUFFER_SIZE);
        seg_dir = std::make_unique<ReadBufferFromFile>(path+column_name+seg_dir_suffix, DBMS_DEFAULT_BUFFER_SIZE);
    }
    catch(...)
    {
        tryLogCurrentException(getLogger("SegmentBitmapIndexReader"), __PRETTY_FUNCTION__);
        compressed_idx = nullptr;
        seg_tab = nullptr;
        seg_dir = nullptr;
    }
}

}
