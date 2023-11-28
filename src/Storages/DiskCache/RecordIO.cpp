#include <algorithm>
#include <memory>
#include <utility>
#include <string.h>

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/RecordIO.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
}

namespace DB::HybridCache
{
bool MetadataOutputStream::Next(void ** data, int * size)
{
    if (buf_index == block_size)
    {
        if (!writeBuffer())
            return false;
    }

    *data = buffer.data() + buf_index;
    *size = block_size - buf_index;
    buf_index = block_size;
    return true;
}

void MetadataOutputStream::BackUp(int count)
{
    buf_index -= count;
}

bool MetadataOutputStream::writeBuffer()
{
    if (failed)
        return false;

    if (buf_index == 0)
        return true;

    if (offset + buf_index > metadata_size)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "exceeding metadata size limit");

    auto extra_bytes = block_size - buf_index;
    memset(buffer.data() + buf_index, 0, extra_bytes);

    if (device.write(offset, BufferView{block_size, buffer.data()}))
    {
        offset += block_size;
        buf_index = 0;
        return true;
    }
    else
    {
        failed = true;
        return false;
    }
}

Int64 MetadataOutputStream::ByteCount() const
{
    return offset + buf_index;
}

bool MetadataOutputStream::invalidate()
{
    Buffer invalidate_buffer{block_size, block_size};
    memset(invalidate_buffer.data(), 0, block_size);
    return device.write(0, std::move(invalidate_buffer));
}

bool MetadataInputStream::Next(const void ** data, int * size)
{
    if (failed)
        return false;

    if (backup_bytes > 0)
    {
        *data = buffer.data() + buf_index - backup_bytes;
        *size = backup_bytes;
        backup_bytes = 0;
        return true;
    }

    if (offset + block_size > metadata_size)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "exceeding metadata size limit");

    if (!device.read(offset, block_size, buffer.data()))
    {
        failed = true;
        return false;
    }

    buf_index = block_size;
    offset += buf_index;

    *data = buffer.data();
    *size = buf_index;

    return true;
}

void MetadataInputStream::BackUp(int count)
{
    backup_bytes = count;
}

int MetadataInputStream::internalSkip(int count)
{
    int skipped = 0;
    Buffer junk(block_size);
    while (skipped < count)
    {
        int size = std::min(count - skipped, static_cast<int>(block_size));
        if (!device.read(offset + skipped, size, junk.data()))
            return skipped;

        skipped += size;
    }
    return skipped;
}

bool MetadataInputStream::Skip(int count)
{
    if (failed)
        return false;

    if (backup_bytes >= count)
    {
        backup_bytes -= count;
        return true;
    }

    count -= backup_bytes;
    backup_bytes = 0;

    int skipped = internalSkip(count);
    offset += skipped;

    return skipped == count;
}

Int64 MetadataInputStream::ByteCount() const
{
    return offset - backup_bytes;
}

std::unique_ptr<MetadataInputStream> createMetadataInputStream(Device & device, size_t metadata_size)
{
    return std::make_unique<MetadataInputStream>(device, metadata_size);
}

std::unique_ptr<MetadataOutputStream> createMetadataOutputStream(Device & device, size_t metadata_size)
{
    return std::make_unique<MetadataOutputStream>(device, metadata_size);
}
}
