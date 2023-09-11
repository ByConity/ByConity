#pragma once

#include <exception>
#include <IO/Scheduler/IOScheduler.h>

namespace DB::IO::Scheduler {

class NoopScheduler: public IOScheduler {
public:
    NoopScheduler() {}

    virtual OpenedFile open(const String& file_name,
        const std::shared_ptr<RemoteFSReaderOpts>& opts) override;
    virtual void readAsync(FileMeta* file, uint64_t offset, uint64_t size,
        char* buffer, std::unique_ptr<FinishCallback> callback) override;

private:
    FDMap fds_;
};

}
