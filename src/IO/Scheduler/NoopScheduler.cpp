#include <exception>
#include <memory>
#include <IO/RemoteFSReader.h>
#include <IO/Scheduler/NoopScheduler.h>

namespace DB::IO::Scheduler {

OpenedFile NoopScheduler::open(const String& file_name,
        const std::shared_ptr<RemoteFSReaderOpts>& opts) {
    return fds_.ref(file_name, opts);
}

void NoopScheduler::readAsync(FileMeta* file, size_t offset, size_t size,
        char* buffer, std::unique_ptr<FinishCallback> callback) {
    try {
        std::unique_ptr<RemoteFSReader> reader = file->borrowReader(offset);
        reader->seek(offset);

        callback->onSuccess(reader->read(buffer, size));

        file->returnReader(std::move(reader));
    } catch (...) {
        callback->onFailure(std::current_exception());

        // Whether reuse this reader?
    }
}

}
