#pragma once

#include <cstdint>
#include <Common/Throttler.h>
#include <IO/RemoteFSReader.h>
#include <Storages/HDFS/HDFSCommon.h>

namespace DB {

class HDFSRemoteFSReader: public RemoteFSReader {
public:
    explicit HDFSRemoteFSReader(const String& hdfs_path, bool pread = false,
        const HDFSConnectionParams& hdfs_params = HDFSConnectionParams(),
        ThrottlerPtr network_throttler = nullptr);
    virtual ~HDFSRemoteFSReader() override;

    virtual String objectName() const override;

    virtual uint64_t read(char* buffer, uint64_t size) override;
    virtual uint64_t seek(uint64_t offset) override;

    virtual uint64_t offset() const override;

    virtual uint64_t remain() const override {
        return 0;
    }

private:
    void connectIfNeeded();

    bool pread_;

    String hdfs_path_;
    HDFSFSPtr hdfs_fs_;
    hdfsFile hdfs_file_;

    ThrottlerPtr throttler_;
    HDFSConnectionParams hdfs_params_;
};

class HDFSRemoteFSReaderOpts: public RemoteFSReaderOpts {
public:
    HDFSRemoteFSReaderOpts(const HDFSConnectionParams& hdfs_params, bool pread = true):
        pread_(pread), hdfs_params_(hdfs_params) {}

    virtual std::unique_ptr<RemoteFSReader> create(const String& path) override;

private:
    bool pread_;
    HDFSConnectionParams hdfs_params_;
};

}
