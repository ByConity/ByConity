#pragma once
#include <Common/config.h>
#if USE_CLOUDFS

#include <Disks/IDisk.h>
#include <IO/AbstractCloudFS.h>
#include <IO/CloudFS/CFSFileSystem.h>

namespace DB
{
class DiskCloudFSReservation;
class DiskCloudFS final : public IDisk
{
public:
    DiskCloudFS(
        const String & disk_name_,
        const DiskPtr & ufs_disk_,
        const std::shared_ptr<AbstractCloudFS> & cfs_)
        : disk_name(disk_name_)
        , ufs_disk(ufs_disk_)
        , cfs(cfs_)
        , cfs_fs(cfs)
    {
        disk_path = ufs_disk->getPath();
        setDiskWritable();
    }

    const String & getPath() const override { return disk_path; }

    const String & getName() const override { return disk_name; }

    DiskType::Type getType() const override { return DiskType::Type::CLOUDFS; }
    DiskType::Type getInnerType() const override { return ufs_disk->getType(); }

    UInt64 getID() const override;

    ReservationPtr reserve(UInt64 bytes) override;

    DiskStats getTotalSpace(bool with_keep_free = false) const override;
    DiskStats getAvailableSpace() const override;
    DiskStats getUnreservedSpace() const override;

    bool exists(const String & path) const override;
    bool isFile(const String & path) const override;
    bool isDirectory(const String & path) const override;
    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;
    void createDirectories(const String & path) override;
    void clearDirectory(const String & path) override;
    void moveDirectory(const String & from_path, const String & to_path) override;
    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void createFile(const String & path) override;
    void moveFile(const String & from_path, const String & to_path) override;
    void replaceFile(const String & from_path, const String & to_path) override;
    void listFiles(const String & path, std::vector<String> & file_names) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings& settings) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        const WriteSettings& settings) override;
    
    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;
    void removePart(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;
    Poco::Timestamp getLastModified(const String & path) override;

    void setReadOnly(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    bool load(const String & path) const override;

private:
    inline String absolutePath(const String& relative_path) const;

    void assertCFSAvailable() const;

    std::string disk_name;
    std::string disk_path;
    /// std::string ufs_base_path;
    /// HDFSConnectionParams hdfs_params;

    DiskPtr ufs_disk;

    std::shared_ptr<AbstractCloudFS> cfs;
    CFSFileSystem cfs_fs;

    LoggerPtr log = getLogger("DiskCloudFS");
};

class DiskCloudFSReservation: public IReservation
{
public:
    DiskCloudFSReservation(std::shared_ptr<DiskCloudFS> disk_, UInt64 size_): disk(disk_), size(size_) {}

    virtual UInt64 getSize() const override
    {
        return size;
    }

    virtual DiskPtr getDisk(size_t i) const override;

    virtual Disks getDisks() const override
    {
        return {disk};
    }

    virtual void update(UInt64 new_size) override
    {
        size = new_size;
    }

private:
    DiskPtr disk;
    UInt64 size;
};

}
#endif
