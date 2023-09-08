#pragma once

#include <map>
#include <mutex>
#include <vector>
#include <atomic>
#include <shared_mutex>
#include <memory>
#include <IO/RemoteFSReader.h>

namespace DB::IO::Scheduler {

/**
 * @brief Store the meta data of one file, mainly including file descriptor, file name, reference count, etc.
 * 
 */
struct FileMeta {
    FileMeta(uint64_t fd, const String& file_name,
        const std::shared_ptr<RemoteFSReaderOpts>& reader_opts);

    std::unique_ptr<RemoteFSReader> borrowReader(uint64_t offset);
    void returnReader(std::unique_ptr<RemoteFSReader> reader);

    std::atomic<uint32_t> ref_count_;

    uint64_t fd_;
    String file_name_;

    std::shared_ptr<RemoteFSReaderOpts> reader_opts_;
};

class FDMap;

/**
 * @brief Store the pointer to the metadata of one file. One file can have several OpenedFiles.
 * 
 */
struct OpenedFile {
    OpenedFile(FileMeta* file_meta, FDMap* mapping):
        file_meta_(file_meta), mapping_(mapping) {}
    ~OpenedFile();

    OpenedFile(const OpenedFile&) = delete;
    OpenedFile& operator=(const OpenedFile&) = delete;

    FileMeta* file_meta_;
    FDMap* mapping_;
};

/**
 * @brief Store file name to file descriptor and file metadata.
 * 
 */
class FDMap {
public:
    FDMap(): next_fd_(0) {}

    // Open one file by given file name and read options
    OpenedFile ref(const String& file_name, const std::shared_ptr<RemoteFSReaderOpts>& opts);
    void unref(uint64_t fd);

private:
    std::shared_mutex mu_;
    uint64_t next_fd_;
    std::unordered_map<String, uint64_t> name_to_fd_;
    std::unordered_map<uint64_t, std::unique_ptr<FileMeta>> fd_to_meta_;
};

}
