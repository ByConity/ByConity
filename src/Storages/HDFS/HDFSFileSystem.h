#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <fcntl.h>
#include <Core/Types.h>
#include <boost/algorithm/string.hpp>
#include <Poco/Path.h>
#include <Poco/String.h>
#include <Poco/Timestamp.h>
#include <Poco/URI.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/Exception.h>
#include <Common/config.h>
#include "HDFSCommon.h" 

namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
}

class HDFSFileSystem
{
public:
    HDFSFileSystem(
        const HDFSConnectionParams & hdfs_params_, int io_error_num_to_reconnect_);
    ~HDFSFileSystem() = default;
    HDFSFileSystem(const HDFSFileSystem &) = delete;
    HDFSFileSystem & operator=(const HDFSFileSystem &) = delete;

    bool exists(const std::string& path) const;
    bool remove(const std::string& path, bool recursive = false) const;
    ssize_t getFileSize(const std::string& path) const;
    ssize_t getCapacity() const;
    void list(const std::string& path, std::vector<std::string>& files) const;
    int64_t getLastModifiedInSeconds(const std::string& path) const;
    bool renameTo(const std::string& path, const std::string& rpath) const;
    bool createFile(const std::string& path) const;
    bool createDirectory(const std::string& path) const;
    bool createDirectories(const std::string& path) const;
    bool isFile(const std::string& path) const;
    bool isDirectory(const std::string& path) const;
    bool setWriteable(const std::string& path, bool flag = true) const;
    bool canExecute(const std::string& path) const;
    bool setLastModifiedInSeconds(const std::string& path, uint64_t ts) const;

    HDFSFSPtr getFS() const;

    static inline String normalizePath(const String& path);

private:
    void reconnect() const;
    void reconnectIfNecessary() const;
    void handleError [[ noreturn ]] (const String & func) const;

    HDFSConnectionParams hdfs_params;

    mutable std::shared_mutex hdfs_mutex;
    mutable HDFSFSPtr fs;

    mutable std::atomic<int> io_error_num = 0;
    const int io_error_num_to_reconnect;
};

void registerDefaultHdfsFileSystem(
    const HDFSConnectionParams & hdfs_params, const int io_error_num_to_reconnect);
DB::fs_ptr<DB::HDFSFileSystem> & getDefaultHdfsFileSystem();

}
