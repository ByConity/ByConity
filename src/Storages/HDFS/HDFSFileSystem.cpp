#include <Storages/HDFS/HDFSFileSystem.h>
#include <Poco/URI.h>
#include <Poco/Path.h>

namespace DB
{

HDFSFileSystem::HDFSFileSystem(const std::string & hdfs_user, const std::string & nnproxy,
    const int max_fd_num, const int skip_fd_num, const int io_error_num_to_reconnect)
        : builder(createHDFSBuilder(Poco::URI(), hdfs_user, nnproxy))
        , fs(createHDFSFS(builder.get()))
        , MAX_FD_NUM(max_fd_num)
        , SKIP_FD_NUM(skip_fd_num)
        , io_error_num(0)
        , IO_ERROR_NUM_TO_RECONNECT(io_error_num_to_reconnect)
        , fd_to_hdfs_file(MAX_FD_NUM + SKIP_FD_NUM, nullptr)
{
}

void HDFSFileSystem::TryReconnect()
{
    std::unique_lock<std::shared_mutex> lock(hdfs_mutex);
    if (++io_error_num % IO_ERROR_NUM_TO_RECONNECT == 0)
    {
        builder = createHDFSBuilder(Poco::URI());
        fs = createHDFSFS(builder.get());
        io_error_num = 0;
    }
}

int HDFSFileSystem::Open(const std::string& path, int flags, mode_t mode)
{
    (void)mode;

    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    auto fin = hdfsOpenFile(fs.get(), path.c_str(), flags, 0, 0, 0);
    if (!fin) {
        return -1;
    }
    int fd = GetNextFd(path);
    fd_to_hdfs_file[fd] = fin;
    return fd;
}

// TODO: here we do not accquire the lock, is it OK?
// delay release owing to cpu cache flush
int HDFSFileSystem::Close(const int fd)
{
    if (fd < SKIP_FD_NUM || fd >= MAX_FD_NUM)
    {
        throw Exception("Illegal HDFS fd", ErrorCodes::PARAMETER_OUT_OF_BOUND);
    }
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    auto file = fd_to_hdfs_file[fd];
    int res = hdfsCloseFile(fs.get(), file);
    if (res == 0) {
        fd_to_hdfs_file[fd] = nullptr;
    }
    return res;
}

int HDFSFileSystem::Flush(const int fd)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    auto file = GetHDFSFileByFd(fd);
    return hdfsHFlush(fs.get(), file);
}

ssize_t HDFSFileSystem::Read(const int fd, void *buf, size_t count)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    auto fin = GetHDFSFileByFd(fd);
    return hdfsRead(fs.get(), fin, buf, count);
}

ssize_t HDFSFileSystem::Write(const int fd, const void *buf, size_t count)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    auto file = GetHDFSFileByFd(fd);
    return hdfsWrite(fs.get(), file, buf, count);
}

bool HDFSFileSystem::Exists(const std::string& path) const
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return !hdfsExists(fs.get(), path.c_str());
}

bool HDFSFileSystem::Delete(const std::string& path, bool recursive)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return !hdfsDelete(fs.get(), path.c_str(), recursive);
}

ssize_t HDFSFileSystem::GetSize(const std::string& path) const
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs.get(), path.c_str());
    if (!fileInfo) {
        return -1;
    }
    auto res = fileInfo->mSize;
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

ssize_t HDFSFileSystem::GetCapacity() const
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return static_cast<ssize_t>(hdfsGetCapacity(fs.get()));
}

void HDFSFileSystem::List(const std::string& path,
    std::vector<std::string>& filenames) const
{
    int num;
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    hdfsFileInfo* files = hdfsListDirectory(fs.get(), path.c_str(), &num);
    for (int i = 0; i < num; ++i)
    {
        Poco::Path cur_path(files[i].mName);
        filenames.push_back(cur_path.getFileName());
    }
    hdfsFreeFileInfo(files, num);
}

int64_t HDFSFileSystem::GetLastModifiedInSeconds(
    const std::string& path) const
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs.get(), path.c_str());
    if (!fileInfo)
    {
        return -1;
    }
    auto res = fileInfo->mLastMod;
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

bool HDFSFileSystem::RenameTo(const std::string& path,
    const std::string& rpath)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return !hdfsRename(fs.get(), path.c_str(), rpath.c_str());
}

bool HDFSFileSystem::CopyTo(const std::string& path,
    const std::string& rpath)
{
    // not supported, custom one
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    if (!hdfsExists(fs.get(), path.c_str()))
    {
        errno = ENOENT;
        return false;
    }
    if (hdfsExists(fs.get(), rpath.c_str()))
    {
        errno = EEXIST;
        return false;
    }
    auto fin = hdfsOpenFile(fs.get(), path.c_str(), O_RDONLY, 0, 0, 0);
    if (!fin) {
        return false;
    }
    auto fout = hdfsOpenFile(fs.get(), rpath.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
    if (!fout)
    {
        return false;
    }

    char buf[10240];
    int len = 0;
    while ((len = hdfsRead(fs.get(), fin, buf, 10240)) > 0)
    {
        hdfsWrite(fs.get(), fout, buf, len);
    }

    if (len < 0)
    {
        return false;
    }

    hdfsCloseFile(fs.get(), fin);
    hdfsCloseFile(fs.get(), fout);
    return true;
}

bool HDFSFileSystem::MoveTo(const std::string& path,
    const std::string& rpath)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return !hdfsRename(fs.get(), path.c_str(), rpath.c_str());
}

bool HDFSFileSystem::CreateFile(const std::string& path)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    auto file = hdfsOpenFile(fs.get(), path.c_str(), O_WRONLY, 0, 0, 0);
    if (!file)
    {
        return false;
    }
    hdfsCloseFile(fs.get(), file);
    return true;
}

bool HDFSFileSystem::CreateDirectory(const std::string& path)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return !hdfsCreateDirectory(fs.get(), path.c_str());
}

/// Creates a directory(and all parent directories if necessary).
bool HDFSFileSystem::CreateDirectories(const std::string& path)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return !hdfsCreateDirectoryEx(fs.get(), path.c_str(), 493, 1);
}

int HDFSFileSystem::Seek(const int fd, off_t offset, int whence)
{
    if (whence != SEEK_SET)
    {
        throw Exception("Illegal HDFS fd", ErrorCodes::PARAMETER_OUT_OF_BOUND);
    }

    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    auto file = GetHDFSFileByFd(fd);
    if (!hdfsSeek(fs.get(), file, offset))
    {
        return offset;
    }
    return -1;
}

int HDFSFileSystem::GetNextFd(const std::string& path) {
    // SKIP_FD_NUM to (SKIP_FD_NUM + MAX_FD_NUM)
    int index = std::hash<std::string>{}(path) % MAX_FD_NUM;
    auto dumpy = 0;
    auto i = 0;
    // if flag_ has been set by others, we wait for next cycle
    while (flag_.test_and_set(std::memory_order_acquire))
    {
        i++;
        if (i < 32)
        {
            continue;
        }
        else if (i < 100)
        {
            dumpy = i;
            dumpy++;
        }
        else
        {
            // wait so long
            std::this_thread::yield();
        }
    }

    // acquire the flag_ lock
    while (fd_to_hdfs_file[index] != nullptr) {
        index = (index + 1) % MAX_FD_NUM;
    }
    // unlock the flag_
    flag_.clear(std::memory_order_release);

    return index + SKIP_FD_NUM;
}


bool HDFSFileSystem::IsFile(const std::string& path) const
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs.get(), path.c_str());
    if (!fileInfo) {
        return false;
    }
    auto res = (fileInfo->mKind == kObjectKindFile);
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

bool HDFSFileSystem::IsDirectory(const std::string& path) const
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs.get(), path.c_str());
    if (!fileInfo) {
        return false;
    }
    auto res = (fileInfo->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

bool HDFSFileSystem::SetWriteable(const std::string& path, bool flag)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs.get(), path.c_str());
    if (!fileInfo) {
        return false;
    }
    auto mode = (fileInfo->mPermissions);
    hdfsFreeFileInfo(fileInfo, 1);

    if (flag)
    {
        mode = mode | S_IWUSR;
    }
    else
    {
        mode_t wmask = S_IWUSR | S_IWGRP | S_IWOTH;
		mode = mode & ~wmask;
    }

    if (!hdfsChmod(fs.get(), path.c_str(), mode))
    {
        return true;
    }

    return false;
}

bool HDFSFileSystem::CanExecute(const std::string& path) const
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs.get(), path.c_str());
    if (!fileInfo) {
        return false;
    }
    auto res = (fileInfo->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(fileInfo, 1);
    return res;
}

bool HDFSFileSystem::SetSize(const std::string& path, uint64_t size)
{
    int wait;
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return !hdfsTruncate(fs.get(), path.c_str(), size, &wait);
}

bool HDFSFileSystem::SetLastModifiedInSeconds(const std::string& path, uint64_t ts)
{
    std::shared_lock<std::shared_mutex> lock(hdfs_mutex);
    return !hdfsUtime(fs.get(), path.c_str(), ts, -1);
}

}
