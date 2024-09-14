#include <Common/config.h>
#if USE_CLOUDFS

#include <Disks/CloudFS/DiskCloudFS.h>

#include <Disks/DiskFactory.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <IO/CloudFS/ReadBufferFromCFS.h>
#include <IO/CloudFS/WriteBufferFromCFS.h>
#include <IO/ReadBufferFromFileWithNexusFS.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int INCORRECT_DISK_INDEX;
}

class DiskByteCFSDirectoryIterator : public IDiskDirectoryIterator
{
public:
    DiskByteCFSDirectoryIterator(CFSFileSystem & cfs_fs_, const String & disk_path, const String & dir_path)
        : cfs_fs(cfs_fs_), idx(0)
    {
        base_path = std::filesystem::path(disk_path) / dir_path;

        cfs_fs.list(base_path, file_names, file_sizes);
    }

    virtual void next() override { ++idx; }

    virtual bool isValid() const override { return idx < file_names.size(); }

    virtual String path() const override { return base_path / name(); }

    virtual String name() const override
    {
        if (idx >= file_names.size())
        {
            throw Exception("Trying to get file name while iterator reach eof", ErrorCodes::BAD_ARGUMENTS);
        }
        return file_names[idx];
    }

    size_t size() const override { return file_sizes.at(idx); }

private:
    CFSFileSystem & cfs_fs;

    std::filesystem::path base_path;
    size_t idx;
    std::vector<String> file_names;
    std::vector<size_t> file_sizes;
};

DiskPtr DiskCloudFSReservation::getDisk(size_t i) const
{
    if (i != 0)
    {
        throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
    }
    return disk;
}

inline String DiskCloudFS::absolutePath(const String & relative_path) const
{
    return fs::path(disk_path) / relative_path;
}

/// XXX: is it necessary
void DiskCloudFS::assertCFSAvailable() const
{
    if (cfs == nullptr || !cfs->isAvailable())
        throw Exception("CloudFS disk is not available", ErrorCodes::NETWORK_ERROR);
}

UInt64 DiskCloudFS::getID() const
{
    return static_cast<UInt64>(std::hash<String>{}(DiskType::toString(getType())) ^ std::hash<String>{}(getPath()));
}

ReservationPtr DiskCloudFS::reserve(UInt64 bytes)
{
    return std::make_unique<DiskCloudFSReservation>(static_pointer_cast<DiskCloudFS>(shared_from_this()), bytes);
}

DiskStats DiskCloudFS::getTotalSpace([[maybe_unused]]bool with_keep_free) const
{
    return ufs_disk->getTotalSpace(with_keep_free);
}

DiskStats DiskCloudFS::getAvailableSpace() const
{
    return ufs_disk->getAvailableSpace();
}

DiskStats DiskCloudFS::getUnreservedSpace() const
{
    return ufs_disk->getUnreservedSpace();
}

bool DiskCloudFS::exists(const String& path) const
{
    assertCFSAvailable();
    return cfs_fs.exists(absolutePath(path));
}

bool DiskCloudFS::isFile(const String& path) const
{
    assertCFSAvailable();
    return cfs_fs.isFile(absolutePath(path));
}

bool DiskCloudFS::isDirectory(const String & path) const
{
    assertCFSAvailable();
    return cfs_fs.isDirectory(absolutePath(path));
}

size_t DiskCloudFS::getFileSize(const String & path) const
{
    assertCFSAvailable();
    return cfs_fs.getFileSize(absolutePath(path));
}

void DiskCloudFS::createDirectory(const String & path)
{
    assertNotReadonly();
    assertCFSAvailable();
    cfs_fs.createDirectory(absolutePath(path));
}

void DiskCloudFS::createDirectories(const String & path)
{
    assertNotReadonly();
    assertCFSAvailable();
    cfs_fs.createDirectories(absolutePath(path));
}

void DiskCloudFS::clearDirectory(const String & path)
{
    assertNotReadonly();
    assertCFSAvailable();
    std::vector<String> file_names;
    std::vector<size_t> file_sizes;
    cfs_fs.list(absolutePath(path), file_names, file_sizes);
    for (const String & file_name : file_names)
    {
        cfs_fs.remove(fs::path(disk_path) / path / file_name);
    }
}

void DiskCloudFS::moveDirectory(const String & from_path, const String & to_path)
{
    assertNotReadonly();
    assertCFSAvailable();
    cfs_fs.renameTo(absolutePath(from_path), absolutePath(to_path));
}

DiskDirectoryIteratorPtr DiskCloudFS::iterateDirectory(const String & path)
{
    assertCFSAvailable();
    return std::make_unique<DiskByteCFSDirectoryIterator>(cfs_fs, disk_path, path);
}

void DiskCloudFS::createFile(const String & path)
{
    assertNotReadonly();
    assertCFSAvailable();
    cfs_fs.createFile(absolutePath(path));
}

void DiskCloudFS::moveFile(const String & from_path, const String & to_path)
{
    assertNotReadonly();
    assertCFSAvailable();
    cfs_fs.renameTo(absolutePath(from_path), absolutePath(to_path));
}

void DiskCloudFS::replaceFile(const String & from_path, const String & to_path)
{
    assertNotReadonly();
    assertCFSAvailable();
    String from_abs_path = absolutePath(from_path);
    String to_abs_path = absolutePath(to_path);

    if (cfs_fs.exists(to_abs_path))
    {
        String origin_backup_file = to_abs_path + ".old";
        cfs_fs.renameTo(to_abs_path, origin_backup_file);
    }
    cfs_fs.renameTo(from_abs_path, to_abs_path);
}

void DiskCloudFS::listFiles(const String & path, std::vector<String> & file_names)
{
    assertCFSAvailable();
    std::vector<size_t> file_sizes;
    cfs_fs.list(absolutePath(path), file_names, file_sizes);
}

std::unique_ptr<ReadBufferFromFileBase> DiskCloudFS::readFile(const String & path, const ReadSettings& settings) const
{
    if (unlikely(settings.remote_fs_read_failed_injection != 0))
    {
        if (settings.remote_fs_read_failed_injection == -1)
            throw Exception("remote_fs_read_failed_injection is enabled and return error immediately", ErrorCodes::LOGICAL_ERROR);
        else
        {
            LOG_TRACE(log, "remote_fs_read_failed_injection is enabled and will sleep {}ms", settings.remote_fs_read_failed_injection);
            std::this_thread::sleep_for(std::chrono::milliseconds(settings.remote_fs_read_failed_injection));
        }
    }

    String file_path = absolutePath(path);
    std::unique_ptr<ReadBufferFromFileBase> impl;
    /// CloudFS switch should be controlled by table level setting (See MergeTreeSettings.h::enable_cloudfs),
    /// which would be checked while getStoragePolicy() is called,
    /// thus here we don't need to check it again, and we only need to check if CloudFS is available.
    /// XXX: if there is any other scenario that we need to check ReadSettings::enable_cloudfs?
    if (cfs->isAvailable()/*  && settings.enable_cloudfs */)
    {
        CloudFSPtr cfs_ptr = dynamic_pointer_cast<CloudFS>(cfs);
        if (!cfs_ptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expect CloudFSPtr to create ReadBufferFromCFS");
        impl = std::make_unique<ReadBufferFromCFS>(cfs_ptr,
                                                    file_path, 
                                                    settings.remote_fs_prefetch,
                                                    settings.remote_fs_buffer_size);
    }
    else
    {
        LOG_WARNING(log, "CloudFS is not available, fallback to ufs disk {} instead", ufs_disk->getName());
        impl = ufs_disk->readFile(file_path, settings);
    }

    if (settings.enable_nexus_fs)
    {
        auto nexus_fs = Context::getGlobalContextInstance()->getNexusFS();
        if (nexus_fs)
            impl = std::make_unique<ReadBufferFromFileWithNexusFS>(nexus_fs->getSegmentSize(), std::move(impl), *nexus_fs);
    }

    if (settings.remote_fs_prefetch)
    {
        auto global_context = Context::getGlobalContextInstance();
        auto reader = global_context->getThreadPoolReader();
        return std::make_unique<AsynchronousBoundedReadBuffer>(std::move(impl), *reader, settings);
    }

    return impl;
}

std::unique_ptr<WriteBufferFromFileBase> DiskCloudFS::writeFile(const String & path, const WriteSettings& settings)
{
    assertNotReadonly();
    if (unlikely(settings.remote_fs_write_failed_injection != 0))
    {
        if (settings.remote_fs_write_failed_injection == -1)
            throw Exception("remote_fs_write_failed_injection is enabled and return error immediately", ErrorCodes::LOGICAL_ERROR);
        else
        {
            LOG_TRACE(log, "remote_fs_write_failed_injection is enabled and will sleep {}ms", settings.remote_fs_write_failed_injection);
            std::this_thread::sleep_for(std::chrono::milliseconds(settings.remote_fs_write_failed_injection));
        }
    }

    /// The same as readFile() api, see comment above.
    if (cfs->isAvailable()/*  && settings.enable_cloudfs */)
    {
        int write_mode = settings.mode == WriteMode::Append ? (O_APPEND | O_WRONLY) : O_CREAT;
        CloudFSPtr cfs_ptr = dynamic_pointer_cast<CloudFS>(cfs);
        if (!cfs_ptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "expect CloudFSPtr to create WriteBufferFromCFS");
        return std::make_unique<WriteBufferFromCFS>(cfs_ptr, absolutePath(path),
                                                     settings.buffer_size, write_mode);
    }
    else
    {
        LOG_WARNING(log, "CloudFS is not available, fallback to ufs disk {} instead", ufs_disk->getName());
        return ufs_disk->writeFile(absolutePath(path), settings);
    }
}

void DiskCloudFS::removeFile(const String & path)
{
    assertNotReadonly();
    assertCFSAvailable();
    cfs_fs.remove(absolutePath(path), false);
}

void DiskCloudFS::removeFileIfExists(const String & path)
{
    assertNotReadonly();
    assertCFSAvailable();
    String abs_path = absolutePath(path);
    if (cfs_fs.exists(abs_path))
        cfs_fs.remove(abs_path, false);
}

void DiskCloudFS::removeDirectory(const String & path)
{
    assertNotReadonly();
    assertCFSAvailable();
    cfs_fs.remove(absolutePath(path), false);
}

void DiskCloudFS::removeRecursive(const String & path)
{
    assertNotReadonly();
    assertCFSAvailable();
    cfs_fs.remove(absolutePath(path), true);
}

void DiskCloudFS::removePart(const String & path)
{
    assertCFSAvailable();
    try
    {
        removeRecursive(path);
    }
    catch (Poco::FileException &e)
    {
        /// We don't know if this exception is caused by a non-existent path,
        /// so we need to determine it manually
        if (!exists(path)) {
            /// the part has already been deleted, exit
            return;
        }
        throw e;
    }
}

void DiskCloudFS::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    assertCFSAvailable();
    cfs_fs.setLastModifiedInSeconds(absolutePath(path), timestamp.epochTime());
}

Poco::Timestamp DiskCloudFS::getLastModified(const String & path)
{
    assertCFSAvailable();
    auto seconds = cfs_fs.getLastModifiedInSeconds(absolutePath(path));
    return Poco::Timestamp(seconds * 1000 * 1000);
}

void DiskCloudFS::setReadOnly(const String & path)
{
    assertCFSAvailable();
    cfs_fs.setWriteable(absolutePath(path), false);
}

void DiskCloudFS::createHardLink(const String &, const String &)
{
    throw Exception("createHardLink is not supported by DiskCloudFS", ErrorCodes::NOT_IMPLEMENTED);
}

bool DiskCloudFS::load(const String & path) const
{
    assertCFSAvailable();
    cfs_fs.load(absolutePath(path));
    return true;
}

void registerDiskCloudFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context_,
                      const DisksMap & disk_map) -> DiskPtr
    {
        String ufs_disk_name = config.getString(config_prefix + ".ufs_disk");
        if (ufs_disk_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ufs_disk is required for CloudFS config");

        auto find = disk_map.find(ufs_disk_name);
        if (find == disk_map.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                         "Cannot find ufs_disk {} while initializing DiskCloudFS {}", ufs_disk_name, name);

        DiskPtr ufs_disk = find->second;
        if (ufs_disk->getType() != DiskType::Type::ByteHDFS && ufs_disk->getType() != DiskType::Type::HDFS)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only ByteHDFS or HDFS is supported for CloudFS now");

        // initialize cfs
        std::shared_ptr<AbstractCloudFS> cfs = nullptr;
        /// bool cfs_enable = config.getBool("cloudfs.enable", false);
        std::unique_ptr<CfsConf> cfs_conf = CfsConf::parseFromConfig(
            config,
            context_,
            config_prefix,
            context_->getHdfsConnectionParams().formatPath(ufs_disk->getPath()).toString());
        cfs = std::make_shared<CloudFS>(std::move(cfs_conf));

        return std::make_shared<DiskCloudFS>(name, ufs_disk, cfs);
    };

    factory.registerDiskType("cfs", creator);
}

} /// end namespace DB

#endif
