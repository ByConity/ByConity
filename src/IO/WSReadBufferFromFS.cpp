#include <IO/WSReadBufferFromFS.h>
#include <sys/types.h>
#include <common/scope_guard.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents {
    extern const Event WSReadBufferReadFailed;
    extern const Event WSReadBufferReadCount;
    extern const Event WSReadBufferReadBytes;
    extern const Event WSReadBufferReadMicro;
}

namespace DB {

namespace ErrorCodes {
    extern const int NOT_IMPLEMENTED;
}

WSReadBufferFromFS::WSReadBufferFromFS(const String& path, const std::shared_ptr<RemoteFSReaderOpts>& reader_opts,
    IO::Scheduler::IOScheduler* scheduler, size_t buf_size,
    char* existing_memory, size_t alignment, const ThrottlerPtr& throttler):
        ReadBufferFromFileBase(buf_size, existing_memory, alignment),
        file_path_(path), scheduler_(scheduler), file_(scheduler->open(path, reader_opts)),
        offset_(0), throttler_(throttler) {}

bool WSReadBufferFromFS::nextImpl() {
    ProfileEvents::increment(ProfileEvents::WSReadBufferReadCount);

    size_t readed = 0;
    try {
        Stopwatch watch;
        SCOPE_EXIT({ProfileEvents::increment(
            ProfileEvents::WSReadBufferReadMicro, watch.elapsedMicroseconds());});

        std::future<size_t> future = scheduler_->read(file_.file_meta_, offset_,
            internalBuffer().size(), internalBuffer().begin());
        readed = future.get();
    } catch (...) {
        ProfileEvents::increment(ProfileEvents::WSReadBufferReadFailed);
        throw;
    }

    offset_ += readed;
    buffer().resize(readed);

    ProfileEvents::increment(ProfileEvents::WSReadBufferReadBytes, readed);

    if (readed > 0 && throttler_ != nullptr) {
        throttler_->add(readed);
    }

    return readed != 0;
}

off_t WSReadBufferFromFS::seek(off_t off, int whence) {
    if (whence != SEEK_SET) {
        throw Exception("WSReadBufferFromFS only support SEEK_SET",
            ErrorCodes::NOT_IMPLEMENTED);
    }

    if (static_cast<size_t>(off) >= offset_ - buffer().size() &&
            static_cast<size_t>(off) <= offset_) {
        pos = working_buffer.begin() + (off - (offset_ - working_buffer.size()));
        return off;
    } else {
        pos = working_buffer.end();

        offset_ = off;
        return off;
    }
}

}