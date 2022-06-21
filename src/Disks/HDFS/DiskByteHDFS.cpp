#include <memory>
#include <Disks/DiskType.h>
#include <Disks/HDFS/DiskByteHDFS.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DISK_INDEX;
}

DiskPtr DiskByteHDFSReservation::getDisk(size_t i) const
{
    if (i != 0)
    {
        throw Exception("Can't use i != 0 with single disk reservation",
            ErrorCodes::INCORRECT_DISK_INDEX);
    }
    return disk;
}

const String& DiskByteHDFS::getName() const
{
    return disk_name;
}

ReservationPtr DiskByteHDFS::reserve(UInt64 bytes)
{
    return std::make_unique<DiskByteHDFSReservation>(
        static_pointer_cast<DiskByteHDFS>(shared_from_this()), bytes);
}

bool DiskByteHDFS::exists(const String &path) const
{
    return hdfs_fs.exists(absolutePath(path));
}

bool DiskByteHDFS::isFile(const String &path) const
{
    return hdfs_fs.isFile(absolutePath(path));
}

bool DiskByteHDFS::isDirectory(const String &path) const
{
    return hdfs_fs.isDirectory(absolutePath(path));
}

size_t DiskByteHDFS::getFileSize(const String &path) const
{
    return hdfs_fs.getFileSize(absolutePath(path));
}

void DiskByteHDFS::createDirectory(const String &path)
{
    hdfs_fs.createDirectory(absolutePath(path));
}

void DiskByteHDFS::createDirectories(const String &path)
{
    hdfs_fs.createDirectories(absolutePath(path));
}

void DiskByteHDFS::clearDirectory(const String &path)
{
    std::vector<String> file_names;
    hdfs_fs.list(absolutePath(path), file_names);
    for (const String& file_name : file_names)
    {
        hdfs_fs.remove(fs::path(disk_path) / path / file_name);
    }
}

void DiskByteHDFS::moveDirectory(const String& from_path, const String &to_path)
{
    hdfs_fs.renameTo(absolutePath(from_path), absolutePath(to_path));
}

void DiskByteHDFS::createFile(const String &path)
{
    hdfs_fs.createFile(absolutePath(path));
}

void DiskByteHDFS::moveFile(const String &from_path, const String &to_path)
{
    hdfs_fs.renameTo(absolutePath(from_path), absolutePath(to_path));
}

void DiskByteHDFS::replaceFile(const String& from_path, const String& to_path)
{
    String from_abs_path = absolutePath(from_path);
    String to_abs_path = absolutePath(to_path);
    if (hdfs_fs.exists(to_abs_path))
    {
        String origin_backup_file = to_abs_path + ".old";
        hdfs_fs.renameTo(to_abs_path, origin_backup_file);
    }
    hdfs_fs.renameTo(from_abs_path, to_abs_path);
}

void DiskByteHDFS::copy(const String &, const std::shared_ptr<IDisk> &, const String &)
{
    throw Exception("DiskByteHDFS didn't support copy to another disk",
        ErrorCodes::NOT_IMPLEMENTED);
}

void DiskByteHDFS::listFiles(const String & path, std::vector<String> & file_names)
{
    hdfs_fs.list(absolutePath(path), file_names);
}

std::unique_ptr<ReadBufferFromFileBase> DiskByteHDFS::readFile(
    const String & path, size_t buf_size, size_t, size_t, size_t,
    MMappedFileCache *) const
{
    return std::make_unique<ReadBufferFromByteHDFS>(
        path, true, hdfs_params, buf_size);
}

std::unique_ptr<WriteBufferFromFileBase> DiskByteHDFS::writeFile(
    const String & path,
    size_t buf_size,
    WriteMode mode)
{
    int write_mode = mode == WriteMode::Append ? O_APPEND : O_WRONLY;
    return std::make_unique<WriteBufferFromHDFS>(path,
        hdfs_params, buf_size, write_mode);
}

void DiskByteHDFS::removeFile(const String & path)
{
    hdfs_fs.remove(absolutePath(path), false);
}

void DiskByteHDFS::removeFileIfExists(const String &path)
{
    String abs_path = absolutePath(path);
    if (hdfs_fs.exists(abs_path))
    {
        hdfs_fs.remove(abs_path, false);
    }
}

void DiskByteHDFS::removeDirectory(const String & path)
{
    hdfs_fs.remove(absolutePath(path), true);
}

void DiskByteHDFS::removeRecursive(const String &path)
{
    hdfs_fs.remove(absolutePath(path), true);
}

void DiskByteHDFS::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    hdfs_fs.setLastModifiedInSeconds(absolutePath(path), timestamp.epochTime());
}

Poco::Timestamp DiskByteHDFS::getLastModified(const String & path)
{
    auto seconds = hdfs_fs.getLastModifiedInSeconds(absolutePath(path));
    return Poco::Timestamp(seconds * 1000 * 1000);
}

void DiskByteHDFS::setReadOnly(const String & path)
{
    hdfs_fs.setWriteable(absolutePath(path), false);
}

void DiskByteHDFS::createHardLink(const String &, const String &)
{
    throw Exception("createHardLink is not supported by DiskByteHDFS",
        ErrorCodes::NOT_IMPLEMENTED);
}

DiskType::Type DiskByteHDFS::getType() const
{
    return DiskType::Type::ByteHDFS;
}

inline String DiskByteHDFS::absolutePath(const String& relative_path) const
{
    return fs::path(disk_path) / relative_path;
}

}
