#pragma once

#include <memory>

#include <google/protobuf/io/zero_copy_stream.h>

#include <Storages/DiskCache/Device.h>

namespace DB::HybridCache
{
class MetadataOutputStream final : public google::protobuf::io::ZeroCopyOutputStream
{
public:
    explicit MetadataOutputStream(Device & device_, size_t metadata_size_)
        : device{device_}
        , metadata_size{metadata_size_}
        , block_size{device.getIOAlignmentSize() >= kBlockSizeDefault ? device.getIOAlignmentSize() : kBlockSizeDefault}
    {
    }

    ~MetadataOutputStream() override { writeBuffer(); }

    bool Next(void ** data, int * size) override;

    void BackUp(int count) override;

    Int64 ByteCount() const override;

    bool invalidate();

private:
    bool writeBuffer();

    static constexpr size_t kBlockSizeDefault = 4096;
    Device & device;
    size_t metadata_size;
    const size_t block_size;
    UInt64 offset{0};
    UInt32 buf_index{0};
    Buffer buffer{block_size, block_size};

    bool failed{false};
};

class MetadataInputStream final : public google::protobuf::io::ZeroCopyInputStream
{
public:
    MetadataInputStream(Device & device_, size_t metadata_size_)
        : device{device_}
        , metadata_size{metadata_size_}
        , block_size{device.getIOAlignmentSize() >= kBlockSizeDefault ? device.getIOAlignmentSize() : kBlockSizeDefault}
    {
    }

    ~MetadataInputStream() override = default;

    bool Next(const void ** data, int * size) override;

    void BackUp(int count) override;

    bool Skip(int count) override;

    Int64 ByteCount() const override;

private:
    int internalSkip(int count);

    static constexpr size_t kBlockSizeDefault = 4096;
    Device & device;
    size_t metadata_size;
    const size_t block_size;
    UInt64 offset{0};
    UInt32 buf_index{0};
    Buffer buffer{block_size, block_size};

    Int32 backup_bytes{0};

    bool failed{false};
};

std::unique_ptr<MetadataOutputStream> createMetadataOutputStream(Device & device, size_t metadata_size);

std::unique_ptr<MetadataInputStream> createMetadataInputStream(Device & device, size_t metadata_size);
}
