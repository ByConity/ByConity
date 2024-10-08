#pragma once

#include <Common/Logger.h>
#include <cstddef>
#include <thread>
#include <utility>
#include <unordered_map>
#include <Columns/ListIndex.h>
#include <Columns/ColumnNullable.h>
#include <Poco/Logger.h>
#include "Common/FieldVisitors.h"
#include "common/logger_useful.h"
#include "Access/AccessFlags.h"
#include "Core/Types.h"

namespace DB
{
// the same underlying implement as bitmap index, using croaring
using IInsideSegmentIndex = IListIndex;

template <typename VIDTYPE = Int32>
class InsideSegmentIndex : public IInsideSegmentIndex
{
    VIDTYPE vid;

public:
    InsideSegmentIndex() = default;
    explicit InsideSegmentIndex(VIDTYPE vid_) : vid(vid_) {}

    VIDTYPE getVid() const {return vid;}
    void setVid(const VIDTYPE & vid_) { vid = vid_; }
};

// additional id for segments, related to the position in rows
using SegmentBitmapIndexSegmentId = size_t;

using InsideSegmentBitmapIndexPtr = std::shared_ptr<IInsideSegmentIndex>;

template<typename VIDTYPE>
using InsideSegmentIndexes = std::unordered_map<VIDTYPE, InsideSegmentIndex<VIDTYPE>>;

// containing segment indexes and an addtional bitmap to record rows processed
template <typename VIDTYPE = Int32>
struct ListSegmentIndex
{
    SegmentBitmapIndexSegmentId seg_id;
    InsideSegmentIndexes<VIDTYPE> inside_seg_indexes;

    // 1 if the segment is completely set by a thread
    // for space saving
    bool total_seg_covered = false;
    // record the validity of the positions in a segment when total_seg_covered is 0
    BitMap seg_covered;

    ListSegmentIndex() = default;
    explicit ListSegmentIndex(SegmentBitmapIndexSegmentId seg_id_) : seg_id(seg_id_) {}

    SegmentBitmapIndexSegmentId getSegmentId() const {return seg_id;}
    
    void setSegmentId(const SegmentBitmapIndexSegmentId & seg_id_) { seg_id = seg_id_; }

    void totalCover()
    {
        this->total_seg_covered = 1;
    }

    bool getIfCovered()
    {
        return total_seg_covered;
    }

    const BitMap & getCoverMap()
    {
        return seg_covered;
    }

    void mergeCoverMap(const BitMap & cov)
    {
        seg_covered |= cov;
    }

    void coverRange(size_t beg, size_t end)
    {
        this->seg_covered.addRange(beg, end);
    }

    bool containsRange(size_t beg, size_t end)
    {
        return this->seg_covered.containsRange(beg, end);
    }
};

template<typename VIDTYPE>
using ColumnSegmentIndexes = std::unordered_map<SegmentBitmapIndexSegmentId, ListSegmentIndex<VIDTYPE>>;

// write three files, the bitmap file, the table file, the directory file
// the structure is like a skip table, so this directory not literally means the directory
class SegmentBitmapIndexWriter
{
private:
    WriteBufferFromFilePtr seg_idx;
    HashingWriteBufferPtr hash_seg_idx;
    CompressedWriteBufferPtr compressed_seg_idx;
    HashingWriteBufferPtr hash_compressed;
    WriteBufferFromFilePtr seg_tab;
    HashingWriteBufferPtr hash_seg_tab;
    WriteBufferFromFilePtr seg_dir;
    HashingWriteBufferPtr hash_seg_dir;
    bool enable_run_optimization;

    BitmapIndexMode bitmap_index_mode;

    std::vector<String> seg_dir_suffix_vec = {SEGMENT_BITMAP_DIRECTORY_EXTENSION};
    std::vector<String> seg_idx_suffix_vec = {SEGMENT_BITMAP_IDX_EXTENSION};
    std::vector<String> seg_tab_suffix_vec = {SEGMENT_BITMAP_TABLE_EXTENSION};

public:
    // Initialize writers
    SegmentBitmapIndexWriter(String path, String name, BitmapIndexMode bitmap_index_mode_, const bool & enable_run_optimization_ = false);
    SegmentBitmapIndexWriter(String path, BitmapIndexMode bitmap_index_mode_, const bool & enable_run_optimization_ = false);

    void writeRows(size_t total_rows) { writeIntBinary(total_rows, *hash_seg_dir); }
    void writeTabOffset() { writeIntBinary(hash_seg_tab->count(), *hash_seg_dir); }
    // template <typename VIDTYPE> void serialize(IListSegmentIndex & li);
    template <typename VIDTYPE> void serialize(ListSegmentIndex<VIDTYPE> & seg);
    void addToChecksums(MergeTreeData::DataPart::Checksums & checksums, const String & column_name);
};

// initialized when a query need to read segment bitmap index
// this one is the base class
// it read the raw file and returns the bitmaps
class SegmentBitmapIndexReader
{
private:
    String path;
    String column_name;
    BitmapIndexMode bitmap_index_mode ;
    const MergeTreeIndexGranularity & index_granularity;
    size_t segment_granularity;
    [[maybe_unused]] size_t serializing_granularity;
    std::unique_ptr<CompressedReadBufferFromFile> compressed_idx;
    ReadBufferFromFilePtr seg_tab;
    ReadBufferFromFilePtr seg_dir;
    ReadBufferFromFileBase::ProfileCallback profile_callback;

    std::vector<String> seg_dir_suffix_vec = {SEGMENT_BITMAP_DIRECTORY_EXTENSION};
    std::vector<String> seg_idx_suffix_vec = {SEGMENT_BITMAP_IDX_EXTENSION};
    std::vector<String> seg_tab_suffix_vec = {SEGMENT_BITMAP_TABLE_EXTENSION};

public:
    SegmentBitmapIndexReader(String path, String name, BitmapIndexMode bitmap_index_mode_, const MergeTreeIndexGranularity &  index_granularity, size_t segment_granularity, size_t serializing_granularity);
    ~SegmentBitmapIndexReader() = default;
    
    // seek based on seg_dir seg_tab and seg_idx
    template <typename VIDTYPE>
    void deserialize(VIDTYPE vid, IInsideSegmentIndex & li, MarkRanges mark_ranges);
    
    template <typename VIDTYPE>
    bool deserializeVids(std::unordered_set<VIDTYPE> & vids, std::vector<InsideSegmentBitmapIndexPtr> & res_indexes, InsideSegmentBitmapIndexPtr & list_index, MarkRanges mark_ranges);

    void init();
    bool valid() { return (compressed_idx && seg_tab && seg_dir); }
};

class ISegmentBitmapColumnListIndexes
{
    protected:
        String path;
        String colname; // optional
        //size_t offset; // record previos domain if multiple blocks are built(merge case)
        //size_t total_rows;
        bool enable_run_optimization;
        std::unique_ptr<SegmentBitmapIndexWriter> segment_bitmap_index_writer;
    public:
        ISegmentBitmapColumnListIndexes(const String & path_, const String & colname_, const bool & enable_run_optimization_ = false)
            : path(path_), colname(colname_), /*offset(0), total_rows(0),*/ enable_run_optimization(enable_run_optimization_){}

    virtual void asyncAppendColumnData(ColumnPtr col) = 0;

    virtual void serialize(SegmentBitmapIndexWriter & ) = 0;

    virtual void deserialize(SegmentBitmapIndexReader &) = 0;

    virtual void finalize() = 0;

    virtual void addToChecksums(MergeTreeData::DataPart::Checksums & checksums) = 0;

    virtual String getPath() const { return path; }

    LoggerPtr log = getLogger("SegmentBitmapColumnListIndexes");

    virtual ~ISegmentBitmapColumnListIndexes() = default;
};

struct SegmentBitmapBuildTask
{
    SegmentBitmapBuildTask() = default;
    // the offset of the given block
    size_t global_bias = 0;
    // the offset in the given block
    size_t start_offset = 0;
    size_t row_num = 0;
};


template <typename VIDTYPE = Int32>
struct SegmentBitmapBuildTaskHolder
{
    std::condition_variable cond;
    std::mutex mtx;

    // global_bias is used for recording the current offset among insert blocks.
    size_t global_bias = 0;
    // Since we will use multiple threads for building bitmap, the current offset of each thread should
    // assigned uniformly.
    size_t total_rows = 0;

    std::vector<std::shared_ptr<SegmentBitmapBuildTask>> build_tasks;
    std::queue<std::shared_ptr<SegmentBitmapBuildTask>> free_tasks;
    size_t max_size = 0;
    explicit SegmentBitmapBuildTaskHolder(const size_t max_size_)
        : max_size(max_size_)
    {
        //build_tasks = std::vector<std::shared_ptr<BuildTask>>(max_size, std::make_shared<BuildTask>());
        for (size_t i = 0; i < max_size; ++i)
            build_tasks.push_back(std::make_shared<SegmentBitmapBuildTask>());

        for (const auto& task : build_tasks)
        {
            free_tasks.push(task);
        }

        if (max_size == 0)
            throw Exception("Initialize BuildTaskHolder with max_size = 0", ErrorCodes::LOGICAL_ERROR);
    }

    // consume a task from free_tasks
    std::shared_ptr<SegmentBitmapBuildTask> getTask(size_t offset, size_t row_num)
    {
        std::unique_lock<std::mutex> lock(mtx);
        cond.wait(lock, [this](){
            return !free_tasks.empty();
        });

        std::shared_ptr<SegmentBitmapBuildTask> ret_task = free_tasks.front();
        ret_task->global_bias = global_bias;
        ret_task->start_offset = offset;
        ret_task->row_num = row_num;
        free_tasks.pop();

        total_rows += row_num;

        return ret_task;
    }

    void addGlobalBias(size_t row_num)
    {
        std::unique_lock<std::mutex> lock(mtx);

        global_bias += row_num;
    }

    void addTask(std::shared_ptr<SegmentBitmapBuildTask> task)
    {
        std::unique_lock<std::mutex> lock(mtx);
        cond.wait(lock, [this](){
            return free_tasks.size() < max_size;
        });

        free_tasks.push(task);
        cond.notify_all();
    }
};


template <typename VIDTYPE = Int32>
class SegmentBitmapColumnListIndexes : public ISegmentBitmapColumnListIndexes
{
    // for finalizing, can be recognized as the bitmap written to the file system
    ColumnSegmentIndexes<VIDTYPE> final_indexes;
    std::shared_ptr<SegmentBitmapBuildTaskHolder<VIDTYPE>> build_tasks_holder;
    // thread related attributes
    std::unique_ptr<ThreadPool> thread_pool;

    // granulairties
    // configured in the settings
    size_t index_granularity;
    size_t segment_granularity;
    size_t serializing_granularity;

    BitmapIndexMode bitmap_index_mode;

    ThreadGroupStatusPtr thread_group;

public:
    SegmentBitmapColumnListIndexes(const String& path_, const String & colname_, const bool enable_run_optimization_ = false, const size_t max_parallel_threads = 1, size_t index_granularity_ = 8192,  size_t segment_granularity_ = 65536,  size_t serializing_granularity_ = 65536, BitmapIndexMode bitmap_index_mode_ = BitmapIndexMode::ROW) :
                        ISegmentBitmapColumnListIndexes(path_, colname_, enable_run_optimization_), index_granularity(index_granularity_), segment_granularity(segment_granularity_), serializing_granularity(serializing_granularity_), bitmap_index_mode(bitmap_index_mode_)
                        {
                            // LOG_DEBUG(&Logger::get("SegmentBitmapColumnListIndexes"), "index_granularity_:\t" <<  index_granularity_ <<"\tsegment_granularity_:\t" << segment_granularity_ <<"\tserializing_granularity_:\t" << serializing_granularity_);

                            if (serializing_granularity_ < segment_granularity_)
                            {
                                throw Exception("segment bitmap writing fault: serializing_granularity should be greater than or equal to segment_granularity: ", ErrorCodes::LOGICAL_ERROR);
                            }
                            
                            build_tasks_holder = std::make_shared<SegmentBitmapBuildTaskHolder<VIDTYPE>>(max_parallel_threads);

                            thread_pool = std::make_unique<ThreadPool>(build_tasks_holder->max_size);
                            //thread_pool = std::make_unique<FreeThreadPool>(build_tasks_holder->max_size);
                            thread_group = CurrentThread::getGroup();
                            if (!thread_group)
                            {
                                CurrentThread::initializeQuery();
                                thread_group = CurrentThread::getGroup();
                            }

                            // for parallelism, construct the writer beforehand
                            segment_bitmap_index_writer = std::make_unique<SegmentBitmapIndexWriter>(getPath(), bitmap_index_mode, enable_run_optimization_);
                        }
    // threads call this for adding data
    void appendColumnData(ColumnPtr col, std::shared_ptr<SegmentBitmapBuildTask> task);

    // for number
    void constructColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, size_t bias, size_t offset, size_t row_num, [[maybe_unused]]const ColumnVector<VIDTYPE> * col);
    // for string
    void constructColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, size_t bias, size_t offset, size_t row_num, [[maybe_unused]]const ColumnString * col);
    // for nullable
    void constructColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, size_t bias, size_t offset, size_t row_num, [[maybe_unused]]const ColumnNullable * col);
    // for array, include array(nullable)
    void constructColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, size_t bias, size_t offset, size_t row_num, const ColumnArray * col);

    // adding one element into a segment bitmap index of a column
    inline void setSingleColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, size_t row_id, const VIDTYPE & vid);
    // mark the processed row with an base-length method
    inline void setSegmentCover(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, size_t global_offset, size_t row_num);

    // merging computational results of bitmap
    inline void mergeBitmapToFinal(ColumnSegmentIndexes<VIDTYPE> & column_indexes);
    // write completed segments to disk
    inline void diskPrewrite(size_t global_offset, size_t row_num);
    // commit a thread's task
    inline void commitBitmap(ColumnSegmentIndexes<VIDTYPE> & column_indexes,  size_t bias,  size_t offset,  size_t row_num);

    void asyncAppendColumnData(ColumnPtr col) override;

    String dumpDebugInfo() const;

    void serialize(SegmentBitmapIndexWriter & ) override;

    // for debug only
    void deserialize(SegmentBitmapIndexReader &) override;

    void addToChecksums(MergeTreeData::DataPart::Checksums & checksums) override;

    void finalize() override;
};

template<typename VIDTYPE>
inline void SegmentBitmapColumnListIndexes<VIDTYPE>::mergeBitmapToFinal(ColumnSegmentIndexes<VIDTYPE> & column_indexes)
{
    for (auto &column_index : column_indexes)
    {
        // column_index
        SegmentBitmapIndexSegmentId segment_id = column_index.first;
        auto &seg_indexes = column_index.second.inside_seg_indexes;
        auto &seg_covered = column_index.second.getCoverMap();
        
        auto seg_iter = final_indexes.find(segment_id);
        if (seg_iter == final_indexes.end())
            seg_iter = final_indexes.insert({segment_id, ListSegmentIndex<VIDTYPE>(segment_id)}).first;

        // record processed rows
        seg_iter->second.mergeCoverMap(seg_covered);

        auto &final_seg_indexes = seg_iter->second.inside_seg_indexes;

        for (auto &vid_bitmap : seg_indexes)
        {
            auto &vid = vid_bitmap.first;
            auto &bitmap = vid_bitmap.second.getIndex();

            auto vid_iter = final_seg_indexes.find(vid);
            if (vid_iter == final_seg_indexes.end())
                vid_iter = final_seg_indexes.insert({vid, InsideSegmentIndex<VIDTYPE>(vid)}).first;
            
            auto &final_bitmap = vid_iter->second.getIndex();
            final_bitmap |= bitmap;
        }
    }
}

template<typename VIDTYPE>
inline void SegmentBitmapColumnListIndexes<VIDTYPE>::diskPrewrite(const size_t global_offset, const size_t row_num)
{
    size_t beg = global_offset, end = global_offset + row_num - 1;

    SegmentBitmapIndexSegmentId beg_seg = beg / segment_granularity, end_seg = end / segment_granularity;
    
    for (SegmentBitmapIndexSegmentId seg_id = beg_seg; seg_id <= end_seg; ++seg_id)
    {
        auto seg_iter = final_indexes.find(seg_id);
        if (seg_iter == final_indexes.end())
            continue;
        
        if (seg_iter->second.getIfCovered() || seg_iter->second.containsRange(seg_id * segment_granularity, seg_id * segment_granularity + segment_granularity))
        {
            segment_bitmap_index_writer->serialize(final_indexes[seg_id]);

            // release memory usage
            final_indexes.erase(seg_id);
        }
    }
}

template<typename VIDTYPE>
inline void SegmentBitmapColumnListIndexes<VIDTYPE>::commitBitmap(ColumnSegmentIndexes<VIDTYPE> & column_indexes, const size_t bias, const size_t offset, const size_t row_num)
{
    std::unique_lock<std::mutex> lock(build_tasks_holder->mtx);
    
    mergeBitmapToFinal(column_indexes);

    diskPrewrite(bias + offset, row_num);
}

template<typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::setSingleColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, const size_t row_id, const VIDTYPE & vid)
{
    // get segment id
    SegmentBitmapIndexSegmentId segment_id = row_id / segment_granularity;

    auto seg_iter = column_segment_indexes.find(segment_id);
    if (seg_iter == column_segment_indexes.end())
        seg_iter = column_segment_indexes.insert({segment_id, ListSegmentIndex<VIDTYPE>(segment_id)}).first;

    auto &seg_indexes = seg_iter->second.inside_seg_indexes;

    auto vid_iter = seg_indexes.find(vid);
    if (vid_iter == seg_indexes.end())
        vid_iter = seg_indexes.insert({vid, InsideSegmentIndex<VIDTYPE>(vid)}).first;

    auto &bitmap = vid_iter->second.getIndex();

    if (bitmap_index_mode == BitmapIndexMode::ROW)
        bitmap.set(row_id);
    else
        throw Exception("bitmap index mode not support: ", ErrorCodes::LOGICAL_ERROR);
}

template<typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::setSegmentCover(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, const size_t global_offset, const size_t row_num)
{
    // every interval is left-closed and right-closed interval
    size_t beg = global_offset, end = global_offset + row_num - 1;

    SegmentBitmapIndexSegmentId beg_seg = beg / segment_granularity, end_seg = end / segment_granularity;
    
    // end of the first segment and begin of the last segment
    size_t lower_end = beg_seg * segment_granularity + segment_granularity - 1;
    size_t upper_beg = end_seg * segment_granularity;

    // set totally covered segments
    for (SegmentBitmapIndexSegmentId seg_id = beg_seg + 1; seg_id < end_seg; ++seg_id)
    {
        if (column_segment_indexes.find(seg_id) == column_segment_indexes.end())
            column_segment_indexes.insert({seg_id, ListSegmentIndex<VIDTYPE>(seg_id)});
        
        column_segment_indexes[seg_id].totalCover();
    }

    // for partially covered segments
    if (beg_seg < end_seg)
    {
        if (column_segment_indexes.find(beg_seg) == column_segment_indexes.end())
            column_segment_indexes.insert({beg_seg, ListSegmentIndex<VIDTYPE>(beg_seg)});
        if (column_segment_indexes.find(end_seg) == column_segment_indexes.end())
            column_segment_indexes.insert({end_seg, ListSegmentIndex<VIDTYPE>(end_seg)});

        if (beg == beg_seg * segment_granularity)
            column_segment_indexes[beg_seg].totalCover();
        else 
            column_segment_indexes[beg_seg].coverRange(beg, lower_end + 1);

        if (end == end_seg * segment_granularity + segment_granularity - 1)
            column_segment_indexes[end_seg].totalCover();
        else 
            column_segment_indexes[end_seg].coverRange(upper_beg, end + 1);
    }
    // only one segment
    else {
        if (column_segment_indexes.find(beg_seg) == column_segment_indexes.end())
            column_segment_indexes.insert({beg_seg, ListSegmentIndex<VIDTYPE>(beg_seg)});

        if (beg + segment_granularity == end + 1)
            column_segment_indexes[beg_seg].totalCover();
        else
            column_segment_indexes[beg_seg].coverRange(beg, end + 1);
    }
}


template<typename VIDTYPE>
inline void SegmentBitmapColumnListIndexes<VIDTYPE>::constructColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> &column_segment_indexes, size_t bias, size_t offset, size_t row_num, [[maybe_unused]]const ColumnNullable * col)
{
    if constexpr (VIDNumeric<VIDTYPE>)
    {
        const auto * data_numbers = static_cast<const ColumnVector<VIDTYPE> *>(&col->getNestedColumn());
        const auto & data_col = data_numbers->getData();

        size_t global_offset = bias + offset;
        // traverse the rows
        for (size_t i = 0; i<row_num; i++)
        {
            if (col->isNullAt(i))
                continue;

            size_t row_id = global_offset + i;
            const VIDTYPE & vid = data_col[offset + i];
            setSingleColumnSegmentIndexes(column_segment_indexes, row_id, vid);
        }
        setSegmentCover(column_segment_indexes, global_offset, row_num);
    }
    else if constexpr (std::is_same_v<VIDTYPE, String>)
    {
        const auto * data_string = static_cast<const ColumnString *>(&col->getNestedColumn());
        if (!data_string)
            return;
        
        size_t global_offset = bias + offset;
        for (size_t i = 0; i<row_num; i++)
        {
            if (col->isNullAt(i))
                continue;

            size_t row_id = global_offset + i;
            const VIDTYPE & vid = data_string->getDataAt(offset + i).toString();
            setSingleColumnSegmentIndexes(column_segment_indexes, row_id, vid);
        }
        setSegmentCover(column_segment_indexes, global_offset, row_num);
    }
}

template<typename VIDTYPE>
inline void SegmentBitmapColumnListIndexes<VIDTYPE>::constructColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> &column_segment_indexes, size_t bias, size_t offset, size_t row_num, [[maybe_unused]]const ColumnVector<VIDTYPE> * col)
{
    if constexpr (VIDNumeric<VIDTYPE>)
    {
        const auto & data_col = col->getData();

        size_t global_offset = bias + offset;

        // traverse the rows
        for (size_t i = 0; i<row_num; i++)
        {
            size_t row_id = global_offset + i;
            const VIDTYPE & vid = data_col[offset + i];
            
            setSingleColumnSegmentIndexes(column_segment_indexes, row_id, vid);
        }
        setSegmentCover(column_segment_indexes, global_offset, row_num);
    }
}


template<typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::constructColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, size_t bias, size_t offset, size_t row_num, [[maybe_unused]]const ColumnString * col)
{
    if constexpr (std::is_same_v<VIDTYPE, String>)
    {
        size_t global_offset = bias + offset;

        for (size_t i = 0; i<row_num; i++)
        {
            size_t row_id = global_offset + i;
            const VIDTYPE & vid = col->getDataAt(offset + i).toString();
            
            setSingleColumnSegmentIndexes(column_segment_indexes, row_id, vid);
        }

        setSegmentCover(column_segment_indexes, global_offset, row_num);
    }
}


template<typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::constructColumnSegmentIndexes(ColumnSegmentIndexes<VIDTYPE> & column_segment_indexes, size_t bias, size_t offset, size_t row_num, const ColumnArray * col)
{
    const auto & input_offset = col->getOffsets();
    const auto & data_col = col->getData();

    if (data_col.isNullable())
    {
        if constexpr (std::is_same<VIDTYPE, String>::value)
        {
            const auto * data_nullable_string = static_cast<const ColumnNullable *>(&data_col);
            const auto * data_string = static_cast<const ColumnString *>(&data_nullable_string->getNestedColumn());
            if (!data_string)
                return;

            size_t global_offset = bias + offset;

            size_t pre_pos = input_offset[offset - 1];
            for (size_t i = 0; i < row_num; i++)
            {
                size_t row_id = global_offset + i;
                size_t end_pos = input_offset[offset + i];

                for (size_t j = pre_pos; j < end_pos; j++)
                {
                    if (data_nullable_string->isNullAt(j))
                        continue;

                    const String & vid = data_string->getDataAt(j).toString();
                    setSingleColumnSegmentIndexes(column_segment_indexes, row_id, vid);
                }

                pre_pos = end_pos;
            }
            setSegmentCover(column_segment_indexes, global_offset, row_num);
        }
        else
        {
            const auto * data_nullable_numbers = static_cast<const ColumnNullable *>(&data_col);
            const auto * data_numbers = static_cast<const ColumnVector<VIDTYPE> *>(&data_nullable_numbers->getNestedColumn());
            if (!data_numbers)
                return;

            size_t global_offset = bias + offset;

            const auto & data_col_vec = data_numbers->getData();
            //LOG_DEBUG(&Logger::get("appendColumnData"), "use bitmap index id : "<< std::to_string(task->id));
            size_t pre_pos = input_offset[offset - 1];
            for (size_t i = 0; i<row_num; i++)
            {
                // [pre_pos, offsets[i])
                size_t row_id = global_offset + i;
                size_t end_pos = input_offset[offset + i];
                
                for (size_t j = pre_pos; j < end_pos; j++)
                {
                    if (data_nullable_numbers->isNullAt(j))
                        continue;

                    const VIDTYPE & vid = data_col_vec[j];
                    setSingleColumnSegmentIndexes(column_segment_indexes, row_id, vid);
                }

                pre_pos = end_pos;
            }
            setSegmentCover(column_segment_indexes, global_offset, row_num);
        }
        return;
    }

    if constexpr (std::is_same<VIDTYPE, String>::value)
    {
        const auto * data_string = static_cast<const ColumnString *>(&data_col);
        if (!data_string)
            return;

        size_t global_offset = bias + offset;

        size_t pre_pos = input_offset[offset - 1];
        for (size_t i = 0; i < row_num; i++)
        {
            size_t row_id = global_offset + i;
            size_t end_pos = input_offset[offset + i];

            for (size_t j = pre_pos; j < end_pos; j++)
            {
                const String & vid = data_string->getDataAt(j).toString();
                
                setSingleColumnSegmentIndexes(column_segment_indexes, row_id, vid);
            }

            pre_pos = end_pos;
        }
        setSegmentCover(column_segment_indexes, global_offset, row_num);
    }
    else
    {
        const auto * data_numbers = static_cast<const ColumnVector<VIDTYPE> *>(&data_col);
        if (!data_numbers)
            return;

        size_t global_offset = bias + offset;

        const auto & data_col_vec = data_numbers->getData();
        //LOG_DEBUG(&Logger::get("appendColumnData"), "use bitmap index id : "<< std::to_string(task->id));
        size_t pre_pos = input_offset[offset - 1];
        for (size_t i = 0; i<row_num; i++)
        {
            // [pre_pos, offsets[i])
            size_t row_id = global_offset + i;
            size_t end_pos = input_offset[offset + i];
            
            for (size_t j = pre_pos; j < end_pos; j++)
            {
                const VIDTYPE & vid = data_col_vec[j];
                
                setSingleColumnSegmentIndexes(column_segment_indexes, row_id, vid);
            }

            pre_pos = end_pos;
        }
        setSegmentCover(column_segment_indexes, global_offset, row_num);
    }
}

/**
 * Build Bitmap column's bitmap index based on input block, resize the bitmap in
 * case multiple blocks are used to form a part(merge scenario)
 */
template <typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::appendColumnData(ColumnPtr col, std::shared_ptr<SegmentBitmapBuildTask> task)
{
    // read data row by row
    ColumnSegmentIndexes<VIDTYPE> column_indexes;
    size_t offset = task->start_offset;
    size_t bias = task->global_bias;
    size_t row_num = task->row_num;
    if (dynamic_cast<const ColumnArray *>(col.get()))
        constructColumnSegmentIndexes(column_indexes, bias, offset, row_num, dynamic_cast<const ColumnArray *>(col.get()));
    else if (typeid_cast<const typename VIDColumn<VIDTYPE>::Type *>(col.get()))
        constructColumnSegmentIndexes(column_indexes, bias, offset, row_num, typeid_cast<const typename VIDColumn<VIDTYPE>::Type *>(col.get()));
    else if (dynamic_cast<const ColumnNullable *>(col.get()))
        constructColumnSegmentIndexes(column_indexes, bias, offset, row_num, dynamic_cast<const ColumnNullable *>(col.get()));
    else
        throw Exception("Bitmap column " + colname + " type is wrong",  ErrorCodes::LOGICAL_ERROR);

    // add local bitmap to global bitmap and serialize totally covered segments
    commitBitmap(column_indexes, bias, offset, row_num);
    // add finished task to free_tasks
    build_tasks_holder->addTask(task);
}

template <typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::asyncAppendColumnData(ColumnPtr col)
{
    // auto task = build_tasks_holder->getTask(col);
    size_t row_sum = col->size();

    for (size_t offset = 0; offset < row_sum; offset += serializing_granularity)
    {
        auto row_num = std::min(serializing_granularity, row_sum - offset);

        auto task = build_tasks_holder->getTask(offset, row_num);

        auto run_job = [=, this](){
            //DB::ThreadStatus thread_status;
            CurrentThread::attachToIfDetached(thread_group);
            appendColumnData(col, task);
        };

        thread_pool->scheduleOrThrowOnError(run_job);
    }

    build_tasks_holder->addGlobalBias(row_sum);
}

template <typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::serialize(SegmentBitmapIndexWriter & bitmapWriter)
{
    for (auto & id_segment : final_indexes)
    {
        bitmapWriter.serialize<VIDTYPE>(id_segment.second);
    }
}

template <typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::deserialize(SegmentBitmapIndexReader & )
{
    throw Exception("not implemented!", ErrorCodes::LOGICAL_ERROR);
}

template <typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::finalize()
{
    thread_pool->wait();
    
    serialize(*segment_bitmap_index_writer);

    // Write total_rows of a part into seg_dir
    segment_bitmap_index_writer->writeRows(build_tasks_holder->total_rows);
    // Write end of segment table for consistance of deserialization
    // can be seen as an alignment for read
    segment_bitmap_index_writer->writeTabOffset();
}

template <typename VIDTYPE>
void SegmentBitmapColumnListIndexes<VIDTYPE>::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    if (segment_bitmap_index_writer)
        segment_bitmap_index_writer->addToChecksums(checksums, colname);
}

template <typename VIDTYPE>
void SegmentBitmapIndexWriter::serialize(ListSegmentIndex<VIDTYPE> & seg)
{
    // prevent writing null segment 
    bool having_vid = false;

    auto &seg_indexes = seg.inside_seg_indexes;

    // check content
    for (auto & segid_bitmap: seg_indexes)
    {
        auto & li = segid_bitmap.second;
        InsideSegmentIndex<VIDTYPE> * list_index = static_cast<InsideSegmentIndex<VIDTYPE>*>(&li);

        // if (__builtin_expect(!!(list_index), 1))
        if (list_index)
        {
            having_vid = true;
            break;
        }
    }

    if (!having_vid)
        return;

    // write seg_dir
    size_t seg_tab_offset = hash_seg_tab->count();
    writeIntBinary(seg.getSegmentId(), *hash_seg_dir);
    writeIntBinary(seg_tab_offset, *hash_seg_dir);

    for (auto & segid_bitmap: seg_indexes)
    {
        auto & li = segid_bitmap.second;
        InsideSegmentIndex<VIDTYPE> * list_index = static_cast<InsideSegmentIndex<VIDTYPE>*>(&li);

        if (!list_index)
            continue;

        // 1. write mark in irk
        auto & bitmap = li.getIndex();
        // // TODO optimize it without syncing
        hash_compressed->next();
        size_t compressed_offset = hash_seg_idx->count();
        size_t uncompressed_offset = hash_compressed->offset();

        if constexpr (std::is_same<VIDTYPE, String>::value)
            writeStringBinary(list_index->getVid(), *hash_seg_tab);
        // backward compatible
        // Since the first version of bitmap uses Int64 as vids type when it write bitmap of int index
        // We try to cast int to Int64 to be compatible with old data
        else if constexpr (std::is_same<VIDTYPE, Int32>::value)
            writePODBinary(static_cast<Int64>(list_index->getVid()), *hash_seg_tab);
        else
            writePODBinary(list_index->getVid(), *hash_seg_tab);
        writeIntBinary(compressed_offset, *hash_seg_tab);
        writeIntBinary(uncompressed_offset, *hash_seg_tab);

        if (enable_run_optimization)
        {
            bitmap.shrinkToFit();
            bitmap.runOptimize();
        }
        bitmap.serialize(*hash_compressed);
    }

}

// Support BitmapIndexRead for unique vids
template <typename VIDTYPE>
void SegmentBitmapIndexReader::deserialize(VIDTYPE vid, IListIndex& li, MarkRanges mark_ranges)
{
    // files are compaction of all vids and segments in this part
    [[maybe_unused]] off_t compressed_offset = 0, uncompressed_offset = 0;
    [[maybe_unused]] VIDTYPE tmp_vid;
    size_t total_rows = 0;

    if (!compressed_idx || !seg_tab || !seg_dir)
        throw Exception("Cannot deserialize segment bitmap index since there is no inputstream", ErrorCodes::LOGICAL_ERROR);

    compressed_idx->seek(0, 0);
    seg_tab->seek(0, SEEK_SET);
    seg_dir->seek(0, SEEK_SET);

    // get total rows and seg_dir
    // elements in seg_dir_vec: index&1 == 0 ? seg_id : offset
    // in seg_dir_idx: index of seg_id according to the order
    size_t dir_len;
    std::vector <size_t> seg_dir_vec;
    std::vector <size_t> seg_dir_idx;

    if (!seg_dir->eof())
    {
        SegmentBitmapIndexSegmentId seg_id;
        size_t tab_offset;
        while(!seg_dir->eof())
        {
            readIntBinary(seg_id, *seg_dir);
            readIntBinary(tab_offset, *seg_dir);
            seg_dir_vec.push_back(seg_id);
            seg_dir_vec.push_back(tab_offset);
        }

        // number of segments
        dir_len = (seg_dir_vec.size() - 2) >> 1;
        total_rows = seg_dir_vec[dir_len << 1];

        li.setOriginalRows(total_rows);

        for (size_t i = 0; i < dir_len; ++i)
            seg_dir_idx.push_back(i << 1);

        std::sort(seg_dir_idx.begin(), seg_dir_idx.end(), 
            [&] (auto &x, auto &y)
            {
                return seg_dir_vec[x] < seg_dir_vec[y];
            }
        );
    }
    else
    {
        li.setOriginalRows(total_rows);
        return;
    }

    // scan both the mark_ranges and the seg_vec one pass according to the row order
    std::sort(mark_ranges.begin(), mark_ranges.end(),
        [&] (auto &x, auto &y)
        {
            return x.begin < y.begin;
        }
    );

    size_t cur_idx_ptr = 0;
    for (auto &mark_range : mark_ranges)
    {
        // closed interval
        // version for fixed index_granularity in vanilla
        /* 
        size_t seg_beg = mark_range.begin * index_granularity / segment_granularity;
        size_t seg_end = (mark_range.end * index_granularity - 1) / segment_granularity;
        */

        // version of mark_index
        size_t seg_beg = index_granularity.getMarkStartingRow(mark_range.begin) / segment_granularity;
        size_t seg_end = index_granularity.getMarkStartingRow(mark_range.end - 1) / segment_granularity;
        
        while (cur_idx_ptr < dir_len && seg_dir_vec[seg_dir_idx[cur_idx_ptr]] <= seg_end)
        {
            size_t idx = seg_dir_idx[cur_idx_ptr];
            size_t seg_id   = seg_dir_vec[idx];
            size_t tab_beg  = seg_dir_vec[idx + 1];
            size_t tab_end  = seg_dir_vec[idx + 3];
            // segment in the query range
            if (seg_beg <= seg_id)
            {
                // deserialize a segment
                seg_tab->seek(tab_beg, SEEK_SET);

                while (seg_tab->getPosition() < static_cast<__off_t>(tab_end))
                {
                    if constexpr (std::is_same<VIDTYPE, String>::value)
                        readStringBinary(tmp_vid, *seg_tab);
                    // backward compatible
                    // try to read vid of type Int64 instead of template types
                    // since the old version has written vids in type `Int64`
                    // We only deal with `int` type because only `int` type was used
                    else if constexpr (std::is_same<VIDTYPE, Int32>::value)
                    {
                        Int64 backward_compatible_vid;
                        readPODBinary(backward_compatible_vid, *seg_tab);
                        tmp_vid = backward_compatible_vid;
                    }
                    else
                        readPODBinary(tmp_vid, *seg_tab);
                    readIntBinary(compressed_offset, *seg_tab);
                    readIntBinary(uncompressed_offset, *seg_tab);

                    //std::std::cout<<"threadid: " << std::this_thread::get_id() << " vid: "<<vid<<" tmp_vid: "<<tmp_vid<<" ===>total_rows: "<<total_rows<<std::endl;
                    
                    if (tmp_vid == vid)
                    {
                        compressed_idx->seek(compressed_offset, uncompressed_offset);
                        li.getIndex().deserialize(*compressed_idx);
                        break;
                    }
                }
            }
            cur_idx_ptr++;
        }
    }
}

// Support BitmapIndexRead for unique vids
template <typename VIDTYPE>
bool SegmentBitmapIndexReader::deserializeVids(std::unordered_set<VIDTYPE> & vids, std::vector<InsideSegmentBitmapIndexPtr> & res_indexes, InsideSegmentBitmapIndexPtr & list_index, MarkRanges mark_ranges)
{
    // files are compaction of all vids and segments in this part
    [[maybe_unused]] off_t compressed_offset = 0, uncompressed_offset = 0;
    [[maybe_unused]] VIDTYPE tmp_vid;
    size_t total_rows = 0;

    if (!compressed_idx || !seg_tab || !seg_dir)
        throw Exception("Cannot deserialize segment bitmap index since there is no inputstream", ErrorCodes::LOGICAL_ERROR);

    compressed_idx->seek(0, 0);
    seg_tab->seek(0, SEEK_SET);
    seg_dir->seek(0, SEEK_SET);

    // get total rows and seg_dir
    // elements in seg_dir_vec: index&1 == 0 ? seg_id : offset
    // in seg_dir_idx: index of seg_id according to the order
    size_t dir_len;
    std::vector <size_t> seg_dir_vec;
    std::vector <size_t> seg_dir_idx;

    if (!seg_dir->eof())
    {
        SegmentBitmapIndexSegmentId seg_id;
        size_t tab_offset;
        while(!seg_dir->eof())
        {
            readIntBinary(seg_id, *seg_dir);
            readIntBinary(tab_offset, *seg_dir);
            seg_dir_vec.push_back(seg_id);
            seg_dir_vec.push_back(tab_offset);
        }

        // number of segments
        dir_len = (seg_dir_vec.size() - 2) >> 1;
        total_rows = seg_dir_vec[dir_len << 1];

        list_index->setOriginalRows(total_rows);

        for (size_t i = 0; i < dir_len; ++i)
            seg_dir_idx.push_back(i << 1);

        std::sort(seg_dir_idx.begin(), seg_dir_idx.end(), 
            [&] (auto &x, auto &y)
            {
                return seg_dir_vec[x] < seg_dir_vec[y];
            }
        );
    }
    else
    {
        list_index->setOriginalRows(total_rows);
        return false;
    }

    // scan both the mark_ranges and the seg_vec one pass according to the row number order
    std::sort(mark_ranges.begin(), mark_ranges.end(),
        [&] (auto &x, auto &y)
        {
            return x.begin < y.begin;
        }
    );

    std::unordered_set<VIDTYPE> found_vids;
    size_t cur_idx_ptr = 0;
    for (auto &mark_range : mark_ranges)
    {
        // closed interval
        // version for fixed index_granularity in vanilla
        /* 
        size_t seg_beg = mark_range.begin * index_granularity / segment_granularity;
        size_t seg_end = (mark_range.end * index_granularity - 1) / segment_granularity;
        */

        // version of mark_index
        size_t seg_beg = index_granularity.getMarkStartingRow(mark_range.begin) / segment_granularity;
        size_t seg_end = index_granularity.getMarkStartingRow(mark_range.end - 1) / segment_granularity;
        

        while (cur_idx_ptr < dir_len && seg_dir_vec[seg_dir_idx[cur_idx_ptr]] <= seg_end)
        {
            size_t idx = seg_dir_idx[cur_idx_ptr];
            size_t seg_id   = seg_dir_vec[idx];
            size_t tab_beg  = seg_dir_vec[idx + 1];
            size_t tab_end  = seg_dir_vec[idx + 3];
            // segment in the query range
            if (seg_beg <= seg_id)
            {
                // deserialize a segment
                seg_tab->seek(tab_beg, SEEK_SET);
                size_t vids_remain = vids.size();

                while (seg_tab->getPosition() < static_cast<__off_t>(tab_end))
                {
                    if constexpr (std::is_same<VIDTYPE, String>::value)
                        readStringBinary(tmp_vid, *seg_tab);
                    // backward compatible
                    // try to read vid of type Int64 instead of template types
                    // since the old version has written vids in type `Int64`
                    // We only deal with `int` type because only `int` type was used
                    else if constexpr (std::is_same<VIDTYPE, Int32>::value)
                    {
                        Int64 backward_compatible_vid;
                        readPODBinary(backward_compatible_vid, *seg_tab);
                        tmp_vid = backward_compatible_vid;
                    }
                    else
                        readPODBinary(tmp_vid, *seg_tab);
                    readIntBinary(compressed_offset, *seg_tab);
                    readIntBinary(uncompressed_offset, *seg_tab);

                    //std::std::cout<<"threadid: " << std::this_thread::get_id() << " vid: "<<vid<<" tmp_vid: "<<tmp_vid<<" ===>total_rows: "<<total_rows<<std::endl;
                    
                    if (vids.find(tmp_vid) != vids.end())
                    {
                        found_vids.insert(tmp_vid);
                        vids_remain--;
                        InsideSegmentBitmapIndexPtr temp_index = std::make_shared<IInsideSegmentIndex>();
                        compressed_idx->seek(compressed_offset, uncompressed_offset);
                        temp_index->getIndex().deserialize(*compressed_idx);
                        temp_index->setOriginalRows(total_rows);
                        res_indexes.emplace_back(std::move(temp_index));
                    }
                    if (!vids_remain)
                        break;
                }
            }
            cur_idx_ptr++;
        }
    }

    // Check if all vids have been found
    return found_vids.size() == vids.size();
}

}
