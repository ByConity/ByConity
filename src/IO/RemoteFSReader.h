#pragma once

#include <memory>
#include <Core/Types.h>

namespace DB {

class RemoteFSReader {
public:
    virtual ~RemoteFSReader() = default;

    virtual String objectName() const = 0;

    // Return readed size, return value may less than size if encounter eof,
    // otherwise is guarantee return buffer with size
    virtual uint64_t read(char* buffer, uint64_t size) = 0;
    virtual uint64_t seek(uint64_t offset) = 0;

    // This method should be fast
    virtual uint64_t offset() const = 0;

    // This method should be fast, too
    // Remain data in reader
    virtual uint64_t remain() const = 0;
};

struct RemoteFSReaderOpts {
    virtual ~RemoteFSReaderOpts() {}

    virtual std::unique_ptr<RemoteFSReader> create(const String& path) = 0;
};

}
