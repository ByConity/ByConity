#pragma once

#include <exception>
#include <future>
#include <Common/Throttler.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/Scheduler/FDs.h>
#include <IO/Scheduler/IOScheduler.h>

namespace DB {

// PFRAWSReadBufferFromFS's working buffer is subset of internal buffer
// when refill data, so it will trigger nextImpl more frequently
class PFRAWSReadBufferFromFS: public ReadBufferFromFileBase {
public:
    struct Options {
        size_t min_buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE;
        size_t buffer_step_size_ = 16 * 1024;

        ThrottlerPtr throttler_ = nullptr;

        // When buffer usage has reach certain percentage, it will consider as a
        // sequential read
        uint32_t buffer_sequential_read_threshold_pct_ = 70;

        // Prefetch options

        // Trigger prefetch if cold read(read without usable prefetch data) happened
        bool cold_read_trigger_prefetch_ = true;
        // Only trigger prefetch when sequential read count has reach certian threshold
        uint32_t prefetch_trigger_sequential_read_count_ = 1;
        // Triggger prefetch when remain size is lower than certain size
        uint32_t prefetch_trigger_remain_size_ = 64 * 1024;
        // Trigger prefetch when remain size is lower than certian percentage
        uint32_t prefetch_trigger_remain_pct_ = 25;

        // Read ahead options

        // Only trigger read ahead when sequential read count has reach certain threshold
        uint32_t read_ahead_trigger_sequential_read_count_ = 1;
        // new_buffer_size / old_buffer_size in percentage
        uint32_t buffer_expand_pct_ = 150;
        // Max buffer expand times
        uint32_t max_buffer_expand_times_ = 8;
    };

    PFRAWSReadBufferFromFS(const String& path, const std::shared_ptr<RemoteFSReaderOpts>& reader_opts,
        IO::Scheduler::IOScheduler* scheduler, const Options& opts);

    virtual bool nextImpl() override;

    virtual off_t getPosition() override {
        return buffer_end_offset_ - (internal_buffer.end() - pos);
    }

    virtual String getFileName() const override {
        return file_info_->file_.file_meta_->file_name_;
    }

    virtual off_t seek(off_t off, int whence) override;

    bool supportsReadAt() override { return true; }
    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) override;

private:
    struct PrefetchTask {
        PrefetchTask(uint64_t offset, uint64_t size): future_(promise_.get_future()),
            offset_(offset), size_(size), mem_(size) {}

        std::promise<uint64_t> promise_;
        std::future<uint64_t> future_;
        uint64_t offset_;
        uint64_t size_;
        Memory<> mem_;
    };

    struct FileInfo {
        FileInfo(const String& path, IO::Scheduler::IOScheduler* scheduler,
            const std::shared_ptr<RemoteFSReaderOpts>& reader_opts):
                scheduler_(scheduler), file_(scheduler->open(path, reader_opts)) {}

        IO::Scheduler::IOScheduler* scheduler_;
        IO::Scheduler::OpenedFile file_;
    };

    class PrefetchFinishCallback: public IO::Scheduler::FinishCallback {
    public:
        PrefetchFinishCallback(const std::shared_ptr<FileInfo>& file_info,
            const std::shared_ptr<PrefetchTask>& task, const ThrottlerPtr& throttler):
                file_info_(file_info), prefetch_task_(task), throttler_(throttler) {}

        virtual void onSuccess(uint64_t readed) override;
        virtual void onFailure(const std::exception_ptr& exception) override;

        std::shared_ptr<FileInfo> file_info_;

        std::shared_ptr<PrefetchTask> prefetch_task_;

        ThrottlerPtr throttler_;
    };

    void updateBufferStatistics();
    void refillWorkingBuffer();
    bool offsetInPrefetchRange(uint64_t offset);
    void utilizePrefetchData(uint64_t offset);
    bool shouldPrefetch();
    void triggerPrefetch(uint64_t offset, uint64_t size);

    const Options opts_;

    std::shared_ptr<FileInfo> file_info_;

    // Start offset current reading operation, update on each buffer refill
    // or seek happened
    size_t current_reading_start_offset_;

    // Total used bytes in current working buffer, update on each buffer
    // refill or seek happened
    size_t used_bytes_in_active_buffer_;

    // Buffer level statistics, updated on each buffer refill happened
    uint32_t sequential_read_count_;
    uint32_t buffer_expanded_times_;
    size_t next_buffer_size_;

    // End offset of current internal buffer
    uint64_t buffer_end_offset_;

    std::shared_ptr<PrefetchTask> prefetch_task_;

    std::optional<uint64_t> hint_file_size_;
};

}
