#include <IO/PFRAWSReadBufferFromFS.h>
#include <cstdint>
#include <future>
#include "Common/ProfileEvents.h"
#include <Common/Stopwatch.h>
#include <common/scope_guard.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace ProfileEvents {
    extern const Event PFRAWSReadBufferReadCount;
    extern const Event PFRAWSReadBufferPrefetchCount;
    extern const Event PFRAWSReadBufferPrefetchUtilCount;
    extern const Event PFRAWSReadBufferPrefetchWaitMicro;
    extern const Event PFRAWSReadBufferRemoteReadCount;
    extern const Event PFRAWSReadBufferRemoteReadBytes;
    extern const Event PFRAWSReadBufferReadMicro;
}

namespace DB {

namespace ErrorCodes {
    extern const int BAD_ARGUMENTS;
}

void PFRAWSReadBufferFromFS::PrefetchFinishCallback::onSuccess(uint64_t readed) {
    prefetch_task_->promise_.set_value(readed);

    if (throttler_ != nullptr) {
        throttler_->add(readed);
    }
}

void PFRAWSReadBufferFromFS::PrefetchFinishCallback::onFailure(const std::exception_ptr& exception) {
    try {
        prefetch_task_->promise_.set_exception(exception);
    } catch (const std::future_error& e) {
        if (e.code() == std::future_errc::promise_already_satisfied) {
            return;
        }
        throw;
    }
}

PFRAWSReadBufferFromFS::PFRAWSReadBufferFromFS(const String& path,
    const std::shared_ptr<RemoteFSReaderOpts>& reader_opts,
    IO::Scheduler::IOScheduler* scheduler, const Options& opts):
        ReadBufferFromFileBase(opts.min_buffer_size_, nullptr, 0),
        opts_(opts), file_info_(std::make_shared<FileInfo>(path, scheduler, reader_opts)),
        current_reading_start_offset_(0), used_bytes_in_active_buffer_(0),
        sequential_read_count_(0), buffer_expanded_times_(0), next_buffer_size_(opts.min_buffer_size_),
        buffer_end_offset_(0), prefetch_task_(nullptr), hint_file_size_(std::nullopt) {}

bool PFRAWSReadBufferFromFS::nextImpl() {
    if (hint_file_size_.has_value() && working_buffer.end() == internal_buffer.end()
            && buffer_end_offset_ >= hint_file_size_.value()) {
        return false;
    }

    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferReadCount);
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferReadMicro,
        watch.elapsedMicroseconds());});

    used_bytes_in_active_buffer_ += working_buffer.size() - current_reading_start_offset_;
    current_reading_start_offset_ = 0;

    assert(working_buffer.begin() >= internal_buffer.begin()
        && working_buffer.end() <= internal_buffer.end()
        && "Working buffer is not a subset of internal buffer");

    if (working_buffer.end() < internal_buffer.end()) {
        // Still more data in working buffer
        size_t remain_size = internal_buffer.end() - working_buffer.end();
        working_buffer = BufferBase::Buffer(working_buffer.end(),
            working_buffer.end() + std::min(remain_size, opts_.buffer_step_size_));
    } else {
        // No more data in internal buffer, trigger buffer refill
        updateBufferStatistics();

        refillWorkingBuffer();
    }

    if (shouldPrefetch()) {
        triggerPrefetch(buffer_end_offset_, next_buffer_size_);
    }

    return working_buffer.size() > 0;
}

void PFRAWSReadBufferFromFS::updateBufferStatistics() {
    uint32_t buffer_used_pct = internal_buffer.empty() ? 0 :
        (100 * used_bytes_in_active_buffer_ / internal_buffer.size());

    used_bytes_in_active_buffer_ = 0;

    if (buffer_used_pct >= opts_.buffer_sequential_read_threshold_pct_) {
        // Read pattern of last buffer is a sequential
        ++sequential_read_count_;
    } else {
        // Read pattern of last buffer is not a sequential
        sequential_read_count_ = 0;

        // Reset read ahead state
        buffer_expanded_times_ = 0;
        next_buffer_size_ = opts_.min_buffer_size_;
    }

    if (sequential_read_count_ >= opts_.read_ahead_trigger_sequential_read_count_) {
        if (buffer_expanded_times_ < opts_.max_buffer_expand_times_) {
            ++buffer_expanded_times_;
            next_buffer_size_ = std::max(opts_.min_buffer_size_,
                next_buffer_size_ * opts_.buffer_expand_pct_ / 100);
        }
    }
}

void PFRAWSReadBufferFromFS::refillWorkingBuffer() {
    working_buffer.resize(0);
    internal_buffer.resize(0);

    if (offsetInPrefetchRange(buffer_end_offset_)) {
        utilizePrefetchData(buffer_end_offset_);
    } else {
        ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferRemoteReadCount);
        ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferRemoteReadBytes,
            next_buffer_size_);

        if (prefetch_task_ != nullptr) {
            if (prefetch_task_->offset_ < buffer_end_offset_ ||
                    prefetch_task_->offset_ - buffer_end_offset_ > next_buffer_size_) {
                prefetch_task_ = nullptr;
            }
        }

        memory.resize(next_buffer_size_);
        std::future<uint64_t> future = file_info_->scheduler_->read(file_info_->file_.file_meta_,
            buffer_end_offset_, next_buffer_size_, memory.data());

        if (opts_.cold_read_trigger_prefetch_) {
            triggerPrefetch(buffer_end_offset_ + next_buffer_size_, next_buffer_size_);
        }

        uint64_t readed = future.get();

        if (readed < next_buffer_size_) {
            hint_file_size_ = hint_file_size_.has_value() ? std::min(hint_file_size_.value(), buffer_end_offset_ + readed)
                : buffer_end_offset_ + readed;
        }

        if (opts_.throttler_ != nullptr) {
            opts_.throttler_->add(readed);
        }

        working_buffer = BufferBase::Buffer(memory.data(),
            memory.data() + std::min(opts_.buffer_step_size_, readed));
        internal_buffer = BufferBase::Buffer(memory.data(), memory.data() + readed);
        pos = working_buffer.begin();
        buffer_end_offset_ += readed;
    }
}

bool PFRAWSReadBufferFromFS::offsetInPrefetchRange(uint64_t offset) {
    return prefetch_task_ != nullptr && offset >= prefetch_task_->offset_
        && offset < prefetch_task_->offset_ + prefetch_task_->size_;
}

void PFRAWSReadBufferFromFS::utilizePrefetchData(uint64_t offset) {
    assert(prefetch_task_ != nullptr && offset >= prefetch_task_->offset_
        && offset < prefetch_task_->offset_ + prefetch_task_->size_);

    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferPrefetchUtilCount);
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferPrefetchWaitMicro,
        watch.elapsedMicroseconds());});

    try {
        uint64_t readed = prefetch_task_->future_.get();

        memory.swap(prefetch_task_->mem_);

        size_t read_start_offset = std::min(readed, offset - prefetch_task_->offset_);
        size_t read_end_offset = std::min(readed, read_start_offset + opts_.buffer_step_size_);

        working_buffer = BufferBase::Buffer(memory.data() + read_start_offset,
            memory.data() + read_end_offset);
        internal_buffer = BufferBase::Buffer(memory.data(), memory.data() + readed);
        pos = working_buffer.begin();

        buffer_end_offset_ = prefetch_task_->offset_ + readed;

        if (readed < prefetch_task_->size_) {
            hint_file_size_ = hint_file_size_.has_value() ? std::min(hint_file_size_.value(), prefetch_task_->offset_ + readed)
                : prefetch_task_->offset_ + readed;
        }

        prefetch_task_ = nullptr;
    } catch (...) {
        prefetch_task_ = nullptr;
        throw;
    }
}

bool PFRAWSReadBufferFromFS::shouldPrefetch() {
    if (prefetch_task_ != nullptr) {
        return false;
    }

    if (sequential_read_count_ < opts_.prefetch_trigger_sequential_read_count_) {
        return false;
    }

    size_t remain_pct = internal_buffer.empty() ? 0 :
        (100 * (internal_buffer.end() - pos)) / internal_buffer.size();
    return (remain_pct > opts_.prefetch_trigger_remain_pct_)
            && (internal_buffer.end() - pos > opts_.prefetch_trigger_remain_size_);
}

void PFRAWSReadBufferFromFS::triggerPrefetch(uint64_t offset, uint64_t size) {
    if (prefetch_task_ != nullptr) {
        return;
    }

    if (hint_file_size_.has_value() && offset >= hint_file_size_.value()) {
        return;
    }

    ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferPrefetchCount);
    ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferRemoteReadCount);
    ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferRemoteReadBytes,
        size);

    prefetch_task_ = std::make_shared<PrefetchTask>(offset, size);
    std::unique_ptr<PrefetchFinishCallback> callback = std::make_unique<PrefetchFinishCallback>(
        file_info_, prefetch_task_, opts_.throttler_);

    file_info_->scheduler_->readAsync(file_info_->file_.file_meta_, offset, size,
        prefetch_task_->mem_.data(), std::move(callback));
}

off_t PFRAWSReadBufferFromFS::seek(off_t off, int whence) {
    if (whence != SEEK_SET) {
        throw Exception("PFRAWSReadBufferFromFS only support SEEK_SET",
            ErrorCodes::BAD_ARGUMENTS);
    }

    Stopwatch watch;
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::PFRAWSReadBufferReadMicro,
        watch.elapsedMicroseconds());});

    uint64_t offset = off;

    uint64_t buffer_begin_offset = buffer_end_offset_ - internal_buffer.size();
    if (offset >= buffer_begin_offset && offset <= buffer_end_offset_) {
        used_bytes_in_active_buffer_ += pos - working_buffer.begin()
            - current_reading_start_offset_;
        current_reading_start_offset_ = 0;

        size_t buffer_start_offset = offset - buffer_begin_offset;
        size_t buffer_end_offset = std::min(buffer_start_offset + opts_.buffer_step_size_,
            internal_buffer.size());
        
        working_buffer = BufferBase::Buffer(internal_buffer.begin() + buffer_start_offset,
            internal_buffer.begin() + buffer_end_offset);
        pos = working_buffer.begin();
    } else {
        if (offsetInPrefetchRange(offset)) {
            used_bytes_in_active_buffer_ += pos - working_buffer.begin()
                - current_reading_start_offset_;

            updateBufferStatistics();

            utilizePrefetchData(offset);
        } else {
            current_reading_start_offset_ = 0;
            used_bytes_in_active_buffer_ = 0;
            sequential_read_count_ = 0;
            buffer_expanded_times_ = 0;
            next_buffer_size_ = opts_.min_buffer_size_;
            buffer_end_offset_ = offset;

            working_buffer.resize(0);
            internal_buffer = working_buffer;

            pos = working_buffer.end();
        }

        if (opts_.cold_read_trigger_prefetch_) {
            triggerPrefetch(buffer_end_offset_, next_buffer_size_);
        }
    }

    if (shouldPrefetch()) {
        triggerPrefetch(buffer_end_offset_, next_buffer_size_);
    }

    return offset;
}

size_t PFRAWSReadBufferFromFS::readBigAt(char * to, size_t n, size_t range_begin,
        [[maybe_unused]]const std::function<bool(size_t)> & progress_callback) {
    return file_info_->scheduler_->read(file_info_->file_.file_meta_, range_begin, n, to).get();
}

}
