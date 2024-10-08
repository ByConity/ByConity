#include <random>
#include <fmt/format.h>
#include <hdfs/HdfsEvent.h>
#include <common/logger_useful.h>
#include <IO/HDFSRemoteFSReader.h>
#include <Common/ProfileEvents.h>
#include <Storages/HDFS/HDFSFileSystem.h>

namespace ProfileEvents
{
    extern const Event NetworkReadBytes;
    extern const Event ReadBufferFromHdfsRead;
    extern const Event ReadBufferFromHdfsReadBytes;
    extern const Event ReadBufferFromHdfsReadFailed;
    extern const Event HdfsFileOpen;
    extern const Event HDFSReadElapsedMicroseconds;
    extern const Event HdfsGetBlkLocMicroseconds;
    extern const Event HdfsSlowNodeCount;
    extern const Event HdfsFailedNodeCount;
}

namespace DB {

namespace ErrorCodes {
    extern const int HDFS_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

static void ReadBufferFromHdfsCallBack(const hdfsEvent & event)
{
    LOG_TRACE(getLogger("ReadBufferFromHDFS"), fmt::format("get event {} with value {}",
        event.eventType, event.value));
    switch (event.eventType)
    {
        case Hdfs::Event::HDFS_EVENT_SLOWNODE:
            ProfileEvents::increment(ProfileEvents::HdfsSlowNodeCount, 1);
            break;
        case Hdfs::Event::HDFS_EVENT_FAILEDNODE:
            ProfileEvents::increment(ProfileEvents::HdfsFailedNodeCount, 1);
            break;
        case Hdfs::Event::HDFS_EVENT_GET_BLOCK_LOCATION:
            ProfileEvents::increment(ProfileEvents::HdfsGetBlkLocMicroseconds, event.value / 1000);
            break;
        default:
            break;
    }
}

HDFSRemoteFSReader::HDFSRemoteFSReader(const String& hdfs_path, bool pread,
    const HDFSConnectionParams& hdfs_params, ThrottlerPtr network_throttler):
        pread_(pread), hdfs_path_(Poco::URI(hdfs_path).getPath()),
        hdfs_fs_(hdfs_params.conn_type == HDFSConnectionParams::CONN_NNPROXY ? nullptr : getDefaultHdfsFileSystem()->getFS()),
        hdfs_file_(nullptr), throttler_(network_throttler), hdfs_params_(hdfs_params) {}

HDFSRemoteFSReader::~HDFSRemoteFSReader() {
    if (hdfs_file_ != nullptr) {
        hdfsCloseFile(hdfs_fs_.get(), hdfs_file_);
        hdfs_file_ = nullptr;
    }
}

String HDFSRemoteFSReader::objectName() const {
    return hdfs_path_;
}

uint64_t HDFSRemoteFSReader::read(char* buffer, uint64_t size) {
    if (size == 0) {
        return size;
    }

    connectIfNeeded();

    uint64_t readed = 0;
    do {
        int ret = -1;
        Stopwatch watch;
        if (pread_) {
            ret = hdfsPRead(hdfs_fs_.get(), hdfs_file_, buffer + readed,
                size - readed);
        } else {
            ret = hdfsRead(hdfs_fs_.get(), hdfs_file_, buffer + readed,
                size - readed);
        }
        ProfileEvents::increment(ProfileEvents::ReadBufferFromHdfsRead);
        ProfileEvents::increment(ProfileEvents::HDFSReadElapsedMicroseconds, watch.elapsedMicroseconds());

        if (ret > 0) {
            // Read partial data, read again
            ProfileEvents::increment(ProfileEvents::ReadBufferFromHdfsReadBytes, ret);
            readed += ret;
        } else if (ret == 0) {
            // Encounter eof
            return readed;
        } else {
            // Read failed, throw
            ProfileEvents::increment(ProfileEvents::ReadBufferFromHdfsReadFailed);
            throw Exception("Failed to read hdfs file: " + hdfs_path_ + ", Error: "
                + String(hdfsGetLastError()), ErrorCodes::HDFS_ERROR);
        }
    } while (readed < size);

    return readed;
}

uint64_t HDFSRemoteFSReader::seek(uint64_t offset) {
    connectIfNeeded();

    if (!hdfsSeek(hdfs_fs_.get(), hdfs_file_, offset)) {
        return offset;
    } else {
        throw Exception("Cannot seek to the offset, Error: "
            + String(hdfsGetLastError()), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    }
}

uint64_t HDFSRemoteFSReader::offset() const {
    if (hdfs_fs_ == nullptr || hdfs_file_ == nullptr) {
        return 0;
    }
    int64_t ret = hdfsTell(hdfs_fs_.get(), hdfs_file_);
    if (ret == -1) {
        throw Exception(fmt::format("Fialed to get hdfs file position, Error: {}",
            String(hdfsGetLastError())), ErrorCodes::HDFS_ERROR);
    }
    return ret;
}

void HDFSRemoteFSReader::connectIfNeeded() {
    if (hdfs_fs_ == nullptr) {
        HDFSBuilderPtr builder = hdfs_params_.createBuilder(Poco::URI(hdfs_path_));
        hdfs_fs_ = createHDFSFS(builder.get());
        if (hdfs_fs_ == nullptr) {
            throw Exception("Unable to connect to hdfs with " + hdfs_params_.toString()
                + ", Error: " + String(hdfsGetLastError()), ErrorCodes::HDFS_ERROR);
        }
    }

    if (hdfs_file_ == nullptr) {
        for (int retry = 0; retry < 2 && hdfs_file_ == nullptr; ++retry) {
            ProfileEvents::increment(ProfileEvents::HdfsFileOpen);
            hdfs_file_ = hdfsOpenFile(hdfs_fs_.get(), hdfs_path_.c_str(), O_RDONLY, 0, 0, 0, ReadBufferFromHdfsCallBack);
        }

        if (hdfs_file_ == nullptr) {
            throw Exception("Unable to open file " + hdfs_path_ + ", Error: "
                + String(hdfsGetLastError()), ErrorCodes::HDFS_ERROR);
        }
    }
}

std::unique_ptr<RemoteFSReader> HDFSRemoteFSReaderOpts::create(const String& path) {
    return std::make_unique<HDFSRemoteFSReader>(path, pread_, hdfs_params_);
}

}
