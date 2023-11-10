#include <fmt/format.h>
#include <common/defines.h>
#include <Common/Exception.h>
#include <IO/Scheduler/FDs.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <IO/RemoteFSReader.h>
#include <IO/Scheduler/ReaderCache.h>

namespace DB {

namespace ErrorCodes {
    extern const int LOGICAL_ERROR;
}

namespace IO::Scheduler {

FileMeta::FileMeta(uint64_t fd, const String& file_name,
    const std::shared_ptr<RemoteFSReaderOpts>& reader_opts):
        ref_count_(1), fd_(fd), file_name_(file_name),
        reader_opts_(reader_opts) {}

std::unique_ptr<RemoteFSReader> FileMeta::borrowReader(uint64_t offset) {
    std::unique_ptr<RemoteFSReader> reader = ReaderCache::instance()
        .shard(file_name_).retrieve(file_name_, offset);

    if (reader == nullptr) {
        reader = reader_opts_->create(file_name_);
    }

    return reader;
}

void FileMeta::returnReader(std::unique_ptr<RemoteFSReader> reader) {
    ReaderCache::instance().shard(file_name_).insert(file_name_, std::move(reader));
}

OpenedFile::~OpenedFile() {
    if (mapping_ != nullptr) {
        mapping_->unref(file_meta_->fd_);
    }
}

OpenedFile FDMap::ref(const String& file_name, const std::shared_ptr<RemoteFSReaderOpts>& opts) {
    // Given file name, return <file descriptor, file metadata>
    auto get_file_meta_without_lock = [this](const String& file) -> std::pair<uint64_t, FileMeta*> {
        auto iter = name_to_fd_.find(file);
        if (iter != name_to_fd_.end()) {
            uint32_t fd = iter->second;
            auto meta_iter = fd_to_meta_.find(fd);
            if (unlikely(meta_iter == fd_to_meta_.end())) {
                throw Exception(fmt::format("fd {} with name {} found in name mapping, "
                    "but missing in meta mapping", fd, file), ErrorCodes::LOGICAL_ERROR);
            }

            return std::pair<uint64_t, FileMeta*>(fd, meta_iter->second.get());
        }
        return std::pair<uint64_t, FileMeta*>(-1, nullptr);
    };

    // Fast path, open existing file
    {
        std::shared_lock<std::shared_mutex> lock(mu_);
        auto [fd, file_meta] = get_file_meta_without_lock(file_name);
        if (file_meta != nullptr) {
            ++file_meta->ref_count_;

            return OpenedFile(file_meta, this);
        }
    }

    // Slow path, open non existing file
    {
        // In case file is opened by another thread
        std::lock_guard<std::shared_mutex> lock(mu_);
        auto [fd, file_meta] = get_file_meta_without_lock(file_name);
        if (file_meta != nullptr) {
            ++file_meta->ref_count_;

            return OpenedFile(file_meta, this);
        }

        fd = next_fd_++;
        name_to_fd_[file_name] = fd;
        fd_to_meta_[fd] = std::make_unique<FileMeta>(fd, file_name, std::move(opts));
        return OpenedFile(fd_to_meta_[fd].get(), this);
    }
}

void FDMap::unref(uint64_t fd) {
    std::lock_guard<std::shared_mutex> lock(mu_);

    auto iter = fd_to_meta_.find(fd);
    if (unlikely(iter == fd_to_meta_.end())) {
        throw Exception(fmt::format("fd {} not found in meta mapping", fd),
            ErrorCodes::LOGICAL_ERROR);
    }

    --iter->second->ref_count_;

    if (iter->second->ref_count_ == 0) {
        size_t removed = name_to_fd_.erase(iter->second->file_name_);
        if (unlikely(removed != 1)) {
            throw Exception(fmt::format("file name {} for fd {} is missing in name mapping",
                iter->second->file_name_, fd), ErrorCodes::LOGICAL_ERROR);
        }

        fd_to_meta_.erase(iter);
    }
}

}

}
