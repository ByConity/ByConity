#pragma once

#include <vector>
#include <algorithm>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class CompressedDataIndex
{
public:
    static std::unique_ptr<CompressedDataIndex> openForRead(
        ReadBuffer* reader);
    static std::unique_ptr<CompressedDataIndex> openForWrite(
        WriteBuffer* writer);

    void append(UInt64 compressed_offset, UInt64 uncompressed_size);
    void finalize();

    /// The index must contains all the compressed block's info
    void searchCompressBlock(UInt64 compressed_offset,
        UInt64 uncompressed_distance, UInt64* target_compressed_offset,
        UInt64* target_uncompressed_offset) const;

private:
    void checkWriteMode() const;
    void checkReadMode() const;

    WriteBuffer* writer;

    /// Record compressed block start offset and corresponding uncompressed data size
    std::vector<std::pair<UInt64, UInt64>> offsets;
};

}
