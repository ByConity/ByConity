#include <Compression/CompressedDataIndex.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <fmt/format.h>
#include <Common/typeid_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>

namespace ProfileEvents
{
    extern const Event DeserializeSkippedCompressedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CORRUPTED_DATA;
}

std::shared_ptr<CompressedDataIndex> CompressedDataIndex::openForRead(
    ReadBuffer* reader)
{
    UInt64 version;
    readIntBinary(version, *reader);

    switch(version)
    {
        case 0:
        {
            std::shared_ptr<CompressedDataIndex> idx = std::make_shared<CompressedDataIndex>();
            
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
            return lhs.first < rhs.first || (lhs.first == rhs.first && lhs.second < rhs.second);
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
        if (remain_size <= iter->second)
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

void CompressedDataIndex::copyStateTo(CompressedDataIndex & target) const
{
    target.offsets = offsets;
}

IndexedCompressedBufferReader::IndexedCompressedBufferReader(ReadBuffer& buffer,
    CompressedDataIndex* comp_idx):
        raw_buffer(&buffer), comp_buf(nullptr), cache_comp_buf(nullptr),
        compressed_idx(comp_idx)
{
    if (comp_idx != nullptr)
    {
        if ((comp_buf = typeid_cast<CompressedReadBufferFromFile*>(&buffer)) != nullptr) {}
        else if ((cache_comp_buf = typeid_cast<CachedCompressedReadBuffer*>(&buffer)) != nullptr) {}
    }
}

void IndexedCompressedBufferReader::readStrict(char* buf, size_t size)
{
    raw_buffer->readStrict(buf, size);
}

void IndexedCompressedBufferReader::ignore(size_t size)
{
    if (size == 0)
    {
        return;
    }

    if (comp_buf != nullptr || cache_comp_buf != nullptr)
    {
        size_t compressed_offset = 0;
        size_t uncompressed_offset = 0;
        size_t current_uncompressed_block_size = 0;

        if (comp_buf)
        {
            std::tie(compressed_offset, uncompressed_offset) = comp_buf->position();
            current_uncompressed_block_size = comp_buf->currentBlockUncompressedSize();
        }
        else
        {
            std::tie(compressed_offset, uncompressed_offset) = cache_comp_buf->position();
            current_uncompressed_block_size = cache_comp_buf->currentBlockUncompressedSize();
        }

        size_t block_remain = current_uncompressed_block_size == 0 ? 0
            : current_uncompressed_block_size - uncompressed_offset;
        if (size <= block_remain)
        {
            raw_buffer->ignore(size);
        }
        else
        {
            /// Ignore to another block, worth a seek
            UInt64 uncompressed_offset_to_skip = size + uncompressed_offset;
            UInt64 target_compressed_offset = 0;
            UInt64 target_uncompressed_offset = 0;
            compressed_idx->searchCompressBlock(compressed_offset, uncompressed_offset_to_skip,
                &target_compressed_offset, &target_uncompressed_offset);

            if (target_compressed_offset > compressed_offset && target_uncompressed_offset != size - block_remain)
            {
                ProfileEvents::increment(ProfileEvents::DeserializeSkippedCompressedBytes,
                    target_compressed_offset - compressed_offset);
            }

            if (comp_buf != nullptr)
            {
                comp_buf->seek(target_compressed_offset, target_uncompressed_offset);
            }
            else
            {
                cache_comp_buf->seek(target_compressed_offset, target_uncompressed_offset);
            }
        }
    }
    else
    {
        raw_buffer->ignore(size);
    }
}

}
