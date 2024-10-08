#pragma once

#include "common/types.h"
#include "IO/SeekableReadBuffer.h"

#include <Storages/NexusFS/NexusFS.h>

namespace DB
{

class ReadBufferFromFileWithNexusFS : public ReadBufferFromFileBase
{

public:
    explicit ReadBufferFromFileWithNexusFS(
        size_t buf_size,
        std::unique_ptr<ReadBufferFromFileBase> source_read_buffer,
        NexusFS &nexus_fs);

    ~ReadBufferFromFileWithNexusFS() override = default;

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    IAsynchronousReader::Result readInto(char * data, size_t size, size_t offset, size_t ignore) override;

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) override;

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    std::string getFileName() const override { return file_name; }
    size_t getFileSize() override {return source_read_buffer->getFileSize();}
    off_t getPosition() override { return offset - available(); }
    size_t getFileOffsetOfBufferEnd() const override { return offset; }
    bool supportsReadAt() override { return false; }
    bool isSeekCheap() override { return false; }

private:
    LoggerPtr log = getLogger("ReadBufferFromFileWithNexusFS");

    const String file_name;
    std::unique_ptr<ReadBufferFromFileBase> source_read_buffer;
    NexusFS &nexus_fs;

    off_t offset = 0;
    off_t read_until_position = 0;
};

}
