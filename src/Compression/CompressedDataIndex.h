#pragma once

#include <vector>
#include <algorithm>
#include <Core/Types.h>
#include <Common/LRUCache.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class CompressedDataIndex
{
public:
    static std::shared_ptr<CompressedDataIndex> openForRead(
        ReadBuffer* reader);
    static std::unique_ptr<CompressedDataIndex> openForWrite(
        WriteBuffer* writer);

    void append(UInt64 compressed_offset, UInt64 uncompressed_size);
    void finalize();

    /// The index must contains all the compressed block's info
    void searchCompressBlock(UInt64 compressed_offset,
        UInt64 uncompressed_distance, UInt64* target_compressed_offset,
        UInt64* target_uncompressed_offset) const;

    size_t totalMemorySize() const { return offsets.size() * 16; }

    void copyStateTo(CompressedDataIndex & target) const;

private:
    void checkWriteMode() const;
    void checkReadMode() const;

    WriteBuffer* writer;

    /// Record compressed block start offset and corresponding uncompressed data size
    std::vector<std::pair<UInt64, UInt64>> offsets;
};

class CompressedReadBufferFromFile;
class CachedCompressedReadBuffer;

class IndexedCompressedBufferReader
{
public:
    IndexedCompressedBufferReader(ReadBuffer& buffer, CompressedDataIndex* comp_idx);

    void readStrict(char* buf, size_t size);
    void ignore(size_t size);

private:
    ReadBuffer* raw_buffer;

    CompressedReadBufferFromFile* comp_buf;
    CachedCompressedReadBuffer* cache_comp_buf;

    CompressedDataIndex* compressed_idx;
};

struct CompressedDataIndexWeightFunction
{
    size_t operator()(const CompressedDataIndex& idx) const
    {
        return idx.totalMemorySize();
    }
};

class CompressedDataIndexCache: public LRUCache<String, CompressedDataIndex, std::hash<String>, CompressedDataIndexWeightFunction>
{
public:
    using Base = LRUCache<String, CompressedDataIndex, std::hash<String>, CompressedDataIndexWeightFunction>;
    using Base::Base;
};

}
