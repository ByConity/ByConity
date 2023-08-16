#pragma once

#include <Core/Types.h>
#include <Common/Throttler.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/Scheduler/IOScheduler.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <sys/types.h>

namespace DB {

class WSReadBufferFromFS: public ReadBufferFromFileBase {
public:
    WSReadBufferFromFS(const String& path, const std::shared_ptr<RemoteFSReaderOpts>& reader_opts,
        IO::Scheduler::IOScheduler* scheduler, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char* existing_memory = nullptr, size_t alignment = 0,
        const ThrottlerPtr& throttler = nullptr);

    virtual String getFileName() const override {
        return file_path_;
    }

    virtual off_t getPosition() override {
        return offset_ - (buffer().end() - pos);
    }

    virtual bool nextImpl() override;

    // TODO: How to make return value of doSeek more accurate
    virtual off_t seek(off_t off, int whence) override;
private:
    String file_path_;

    IO::Scheduler::IOScheduler* scheduler_;
    IO::Scheduler::OpenedFile file_;

    size_t offset_;

    ThrottlerPtr throttler_;
};

}
