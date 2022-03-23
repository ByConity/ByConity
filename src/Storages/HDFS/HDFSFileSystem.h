#pragma once

#include <string>
#include <shared_mutex>
#include <Common/Exception.h>
#include <Storages/HDFS/HDFSCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
}

class HDFSFileSystem
{
public:
    HDFSFileSystem(const std::string & hdfs_user, const std::string & hdfs_nnproxy, const int max_fd_num, const int skip_fd_num, const int io_error_num_to_reconnect);
    ~HDFSFileSystem() = default;
    HDFSFileSystem(const HDFSFileSystem&) = delete;
    HDFSFileSystem& operator= (const HDFSFileSystem&) = delete;

    inline hdfsFile GetHDFSFileByFd(int fd) const
    {
        if(fd < SKIP_FD_NUM || fd >= MAX_FD_NUM)
        {
            throw Exception("Illegal HDFS fd", ErrorCodes::PARAMETER_OUT_OF_BOUND);
        }
        return fd_to_hdfs_file[fd];
    }

    void TryReconnect();
    int Open(const std::string& path, int flags, mode_t mode);
    int Close(const int fd);
    int Flush(const int fd);
    ssize_t Read(const int fd, void *buf, size_t count);
    ssize_t Write(const int fd, const void *buf, size_t count);
    bool Exists(const std::string& path) const;
    bool Delete(const std::string& path, bool recursive = false);
    int Seek(const int fd, off_t offset, int whence);
    ssize_t GetSize(const std::string& path) const;
    ssize_t GetCapacity() const;
    void List(const std::string& path, std::vector<std::string>& files) const;
    int64_t GetLastModifiedInSeconds(const std::string& path) const;
    bool RenameTo(const std::string& path, const std::string& rpath);
    bool CopyTo(const std::string& path, const std::string& rpath);
    bool MoveTo(const std::string& path, const std::string& rpath);
    bool CreateFile(const std::string& path);
    bool CreateDirectory(const std::string& path);
    bool CreateDirectories(const std::string& path);
    bool IsFile(const std::string& path) const;
    bool IsDirectory(const std::string& path) const;
    bool SetWriteable(const std::string& path, bool flag = true);
    bool CanExecute(const std::string& path) const;
    bool SetSize(const std::string& path, uint64_t size);
    bool SetLastModifiedInSeconds(const std::string& path, uint64_t ts);

private:
    int GetNextFd(const std::string& path);

private:
    mutable std::shared_mutex hdfs_mutex;
    HDFSBuilderPtr builder;
    HDFSFSPtr fs;

    const int MAX_FD_NUM;
    const int SKIP_FD_NUM;
    std::atomic<int> io_error_num = 0;
    const int IO_ERROR_NUM_TO_RECONNECT;
    std::vector<hdfsFile> fd_to_hdfs_file;
    std::atomic_flag flag_ ATOMIC_FLAG_INIT;
};

}
