#include <Storages/MergeTree/GinIndexDataPartHelper.h>
#include <filesystem>
#include <Core/SettingsEnums.h>
#include <IO/ReadSettings.h>
#include <IO/RemappingReadBuffer.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/FileDiskCacheSegment.h>
#include <Storages/IndexFile/RemappingEnv.h>

namespace ProfileEvents
{
    extern const Event GinIndexCacheHit;
    extern const Event GinIndexCacheMiss;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

GinDataLocalPartHelper::GinDataLocalPartHelper(const IMergeTreeDataPart& part_):
    disk(part_.volume->getDisk()), relative_path(part_.getFullRelativePath())
{
    if (part_.getType() == IMergeTreeDataPart::Type::CNCH)
    {
        throw Exception("Unexpected part type when construct GinDataLocalPartHelper",
            ErrorCodes::LOGICAL_ERROR);
    }
}

GinDataLocalPartHelper::GinDataLocalPartHelper(const DiskPtr& disk_,
    const String& relative_path_):
        disk(disk_), relative_path(relative_path_) {}

std::unique_ptr<SeekableReadBuffer> GinDataLocalPartHelper::readFile(
    const String& file_name, size_t buf_size)
{
    return disk->readFile(std::filesystem::path(relative_path) / file_name,
        ReadSettings {
            .local_fs_buffer_size = buf_size,
            .remote_fs_buffer_size = buf_size
        });
}

std::unique_ptr<WriteBufferFromFileBase> GinDataLocalPartHelper::writeFile(const String& file_name,
    size_t buf_size, WriteMode write_mode)
{
    return disk->writeFile(std::filesystem::path(relative_path) / file_name,
        WriteSettings {
            .buffer_size = buf_size,
            .mode = write_mode
        }
    );
}

bool GinDataLocalPartHelper::exists(const String& file_name) const
{
    return disk->exists(std::filesystem::path(relative_path) / file_name);
}

size_t GinDataLocalPartHelper::getFileSize(const String& file_name) const
{
    return disk->getFileSize(std::filesystem::path(relative_path) / file_name);
}

String GinDataLocalPartHelper::getPartUniqueID() const
{
    return std::filesystem::path(disk->getPath()) / relative_path;
}

GinDataCNCHPartHelper::GinDataCNCHPartHelper(const IMergeTreeDataPartPtr& part_,
    const IDiskCachePtr& cache_, DiskCacheMode mode_):
        cache(cache_), part_checksums(part_->getChecksums()),
        disk(part_->volume->getDisk()), part_rel_path(part_->getFullRelativePath()), mode(mode_), log(getLogger("GinDataCNCHPartHelper"))
{
    if (part_->getType() != IMergeTreeDataPart::Type::CNCH)
    {
        throw Exception("Unexpected part type when construct GinDataCNCHPartHelper",
            ErrorCodes::LOGICAL_ERROR);
    }
}

std::unique_ptr<SeekableReadBuffer> GinDataCNCHPartHelper::readFile(
    const String& file_name, size_t buf_size)
{
    auto file_iter = part_checksums->files.find(file_name);
    if (file_iter == part_checksums->files.end())
    {
        throw Exception(fmt::format("File {} is not in part {} checksums", file_name,
            disk->getPath() + part_rel_path), ErrorCodes::LOGICAL_ERROR);
    }

    size_t offset = file_iter->second.file_offset;
    size_t size = file_iter->second.file_size;
    ReadSettings read_settings {
        .local_fs_buffer_size = buf_size,
        .remote_fs_buffer_size = buf_size
    };

    if (cache != nullptr)
    {
        std::pair<size_t, size_t> data_range = {offset, offset + size};
        auto cache_segment = std::make_shared<FileDiskCacheSegment>(disk, fs::path(part_rel_path) / "data",
            ReadSettings{}, data_range);

        auto [cache_disk, cache_path] = cache->get(cache_segment->getSegmentName());

        if (cache_disk != nullptr && !cache_path.empty())
        {
            LOG_TRACE(log, "GIN index cache hit, part: {}, path: {}", part_rel_path, cache_path);
            ProfileEvents::increment(ProfileEvents::GinIndexCacheHit);

            return cache_disk->readFile(cache_path, read_settings);
        }
        else if (mode == DiskCacheMode::FORCE_DISK_CACHE)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "GIN DiskCache data `{}` is not found but mode is FORCE_DISK_CACHE", cache_segment->getSegmentName());
        }

        auto filtered_segments = cache->getStrategy()->getCacheSegments(cache, {cache_segment});
        cache->cacheSegmentsToLocalDisk(filtered_segments);
    }
    else if (mode == DiskCacheMode::FORCE_DISK_CACHE)
    {
        throw Exception("GIN DiskCache data is not inited but mode is FORCE_DISK_CACHE", ErrorCodes::LOGICAL_ERROR);
    }

    LOG_TRACE(log, "GIN index cache miss, part: {}", part_rel_path);
    ProfileEvents::increment(ProfileEvents::GinIndexCacheMiss);

    std::unique_ptr<ReadBufferFromFileBase> part_reader =
        disk->readFile(part_rel_path + "/data", read_settings);

    return std::make_unique<RemappingReadBuffer>(std::move(part_reader),
        offset, size);
}

std::unique_ptr<WriteBufferFromFileBase> GinDataCNCHPartHelper::writeFile(
    const String&, size_t, WriteMode)
{
    throw Exception("GinDataCNCHPartHelper didn't support write file directly",
        ErrorCodes::LOGICAL_ERROR);
}

size_t GinDataCNCHPartHelper::getFileSize(const String& file_name) const
{
    auto checksum = part_checksums->files.find(file_name);
    if (checksum == part_checksums->files.end())
    {
        throw Exception(fmt::format("File {} is not found in part {} checksums",
            file_name, part_rel_path), ErrorCodes::LOGICAL_ERROR);
    }
    return checksum->second.file_size;
}

String GinDataCNCHPartHelper::getPartUniqueID() const
{
    return part_rel_path;
}

bool GinDataCNCHPartHelper::exists(const String& file_name) const
{
    auto checksum = part_checksums->files.find(file_name);
    return checksum != part_checksums->files.end();
}


}
