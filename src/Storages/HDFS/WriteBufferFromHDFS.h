#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/HDFS/HDFSFileSystem.h>
#include <string>
#include <memory>


namespace DB
{
/** Accepts HDFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class WriteBufferFromHDFS final : public WriteBufferFromFileBase
{

public:
    WriteBufferFromHDFS(
        const std::string & hdfs_name_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = O_WRONLY);
        

    WriteBufferFromHDFS(
        const std::string & hdfs_name_,
        const HDFSConnectionParams & hdfs_params = HDFSConnectionParams::defaultNNProxy(),
        const size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flag = O_WRONLY);

    ~WriteBufferFromHDFS() override;

    void nextImpl() override;

    void sync() override;
    off_t getPositionInFile();
    std::string getFileName() const override;
    void finalize() override;

private:
    struct WriteBufferFromHDFSImpl;
    std::unique_ptr<WriteBufferFromHDFSImpl> impl;
    std::string hdfs_name;
};

}
#endif
