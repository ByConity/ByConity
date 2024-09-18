#include <city.h>
#include <string.h>

#include <common/unaligned.h>
#include <common/types.h>

#include "CompressedStatisticsCollectBuffer.h"
#include <Compression/CompressionFactory.h>

#include <Common/MemorySanitizer.h>
#include <Common/MemoryTracker.h>


namespace DB
{

namespace ErrorCodes
{
}

static constexpr auto CHECKSUM_SIZE{sizeof(CityHash_v1_0_2::uint128)};

void CompressedStatisticsCollectBuffer::nextImpl()
{
    if (!offset())
        return;

    size_t decompressed_size = offset();
    UInt32 compressed_reserve_size = codec->getCompressedReserveSize(decompressed_size);
    compressed_buffer.resize(compressed_reserve_size);
    UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, compressed_buffer.data());

    // FIXME remove this after fixing msan report in lz4.
    // Almost always reproduces on stateless tests, the exact test unknown.
    __msan_unpoison(compressed_buffer.data(), compressed_size);

    total_compressed_size += CHECKSUM_SIZE + compressed_size;
}


CompressedStatisticsCollectBuffer::CompressedStatisticsCollectBuffer(
    CompressionCodecPtr codec_,
    size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), codec(std::move(codec_))
{
}

CompressedStatisticsCollectBuffer::~CompressedStatisticsCollectBuffer()
{
    /// FIXME move final flush into the caller
    next();
}

}
