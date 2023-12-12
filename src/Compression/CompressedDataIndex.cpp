#include <Compression/CompressedDataIndex.h>
#include <algorithm>
#include <memory>
#include <fmt/format.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CORRUPTED_DATA;
}

std::unique_ptr<CompressedDataIndex> CompressedDataIndex::openForRead(
    ReadBuffer* reader)
{
    UInt64 version;
    readIntBinary(version, *reader);

    switch(version)
    {
        case 0:
        {
            std::unique_ptr<CompressedDataIndex> idx = std::make_unique<CompressedDataIndex>();
            
            std::pair<UInt64, UInt64> entry;
            while (!reader->eof())
            {
                readBinary(entry, *reader);
                idx->offsets.push_back(entry);
            }

            return idx;
        }
        default:
            throw Exception(fmt::format("Unknown compressed index version {}", version),
                ErrorCodes::CORRUPTED_DATA);
    }
}

std::unique_ptr<CompressedDataIndex> CompressedDataIndex::openForWrite(
    WriteBuffer* writer)
{
    std::unique_ptr<CompressedDataIndex> idx = std::make_unique<CompressedDataIndex>();
    idx->writer = writer;

    UInt64 version = 0;
    writeIntBinary(version, *(idx->writer));

    return idx;
}

void CompressedDataIndex::append(UInt64 compressed_offset,
    UInt64 uncompressed_size)
{
    checkWriteMode();

    std::pair<UInt64, UInt64> entry(compressed_offset, uncompressed_size);
    writeBinary(entry, *writer);
}

void CompressedDataIndex::finalize()
{
    checkWriteMode();

    writer->finalize();
}

void CompressedDataIndex::searchCompressBlock(UInt64 compressed_offset,
    UInt64 uncompressed_distance, UInt64* target_compressed_offset,
    UInt64* target_uncompressed_offset) const
{
    checkReadMode();

    auto iter = std::lower_bound(offsets.begin(), offsets.end(),
        std::pair<UInt64, UInt64>(compressed_offset, 0),
        [](const std::pair<UInt64, UInt64>& lhs, const std::pair<UInt64, UInt64>& rhs) {
            return lhs.first < rhs.first;
        }
    );
    if (iter == offsets.end() || iter->first != compressed_offset)
    {
        /// We assume the compressed index contains all the data here
        /// maybe support sparse compressed data index?
        throw Exception(fmt::format("Cannot find statistics for compressed block {}",
            compressed_offset), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    }

    for (UInt64 remain_size = uncompressed_distance; iter != offsets.end(); ++iter)
    {
        if (remain_size < iter->second)
        {
            *target_compressed_offset = iter->first;
            *target_uncompressed_offset = remain_size;
            return;
        }
        else
        {
            remain_size -= iter->second;
        }
    }
    throw Exception(fmt::format("Cannot seek for compressed block from {} with uncomrpessed offset {}",
        compressed_offset, uncompressed_distance), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
}

void CompressedDataIndex::checkWriteMode() const
{
    if (unlikely(writer == nullptr))
    {
        throw Exception("CompressedDataIndex is open in read mode but used in write",
            ErrorCodes::LOGICAL_ERROR);
    }
}

void CompressedDataIndex::checkReadMode() const
{
    if (unlikely(writer != nullptr))
    {
        throw Exception("CompressedDataIndex is open in write mode but used in read",
            ErrorCodes::LOGICAL_ERROR);
    }
}

}
