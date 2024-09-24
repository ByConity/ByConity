#include "PartInspector.h"

#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Common/ThreadPool.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <IO/S3Common.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/HashingReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/HDFS/HDFSFileSystem.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>

#include <boost/program_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

#include <sstream>

#define MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE 256

namespace po = boost::program_options;

namespace DB {

namespace ErrorCodes {
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_CONFIG_PARAMETER;
}

class HDFSFSOp: public FSOp {
public:
    explicit HDFSFSOp(const HDFSConnectionParams& params): hdfs_params_(params) {
        registerDefaultHdfsFileSystem(params, 1, 0, 0);
    }

    virtual std::unique_ptr<ReadBufferFromFileBase> read(const String& path) override {
        return std::make_unique<ReadBufferFromByteHDFS>(path, hdfs_params_);
    }

    virtual size_t getSize(const String& path) override {
        return getDefaultHdfsFileSystem()->getFileSize(path);
    }

    virtual bool isFile(const String& path) override {
        return getDefaultHdfsFileSystem()->isFile(path);
    }

    virtual std::vector<String> list(const String& path) override {
        std::vector<String> files;
        std::vector<size_t> file_sizes;
        getDefaultHdfsFileSystem()->list(path, files, file_sizes);
        return files;
    }

private:
    HDFSConnectionParams hdfs_params_;
};

class LocalFSOp: public FSOp {
public:
    virtual std::unique_ptr<ReadBufferFromFileBase> read(const String& path) override {
        return std::make_unique<ReadBufferFromFile>(path);
    }

    virtual size_t getSize(const String& path) override {
        return Poco::File(path).getSize();
    }

    virtual bool isFile(const String& path) override {
        return Poco::File(path).isFile();
    }

    virtual std::vector<String> list(const String& path) override {
        std::vector<String> files;
        for (Poco::DirectoryIterator iter(path); iter != Poco::DirectoryIterator(); ++iter) {
            files.push_back(iter.name());
        }
        return files;
    }
};

class S3FSOp: public FSOp {
public:
    explicit S3FSOp(const S3::S3Config& cfg): s3_util_(cfg.create(), cfg.bucket) {}

    virtual std::unique_ptr<ReadBufferFromFileBase> read(const String& path) override {
        return std::make_unique<ReadBufferFromS3>(s3_util_.getClient(),
            s3_util_.getBucket(), path, ReadSettings(), 3, false);
    }

    virtual size_t getSize(const String& path) override {
        return s3_util_.getObjectSize(path);
    }

    virtual bool isFile([[maybe_unused]]const String& path) override {
        S3::S3Util::S3ListResult result = s3_util_.listObjectsWithPrefix(
            path, std::nullopt, 1);
        if (result.object_names.empty()) {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Didn't find any file start with {}",
                path);
        }
        return result.object_names.front() == path;
    }

    virtual std::vector<String> list(const String& path) override {
        std::vector<String> file_paths;

        S3::S3Util::S3ListResult result;
        do {
            result = s3_util_.listObjectsWithPrefix(path, result.token);
            file_paths.insert(file_paths.end(), result.object_names.begin(),
                result.object_names.end());
        } while (result.has_more);

        return file_paths;
    }

private:
    S3::S3Util s3_util_;
};

String PartInspector::Footer::info() const {
    return fmt::format("IndexOffset: {}, IndexSize: {}, IndexChecksum: {}, "
        "ChecksumOffset: {}, ChecksumSize: {}, ChecksumChecksum: {}, "
        "MetaInfoOffset: {}, MetaInfoSize: {}, MetaInfoChecksum: {}, "
        "UniqueKeyIdxOffset: {}, UniqueKeyIdxSize: {}, UniqueKeyIdxChecksum: {}",
        index_offset_, index_size_, index_checksum_, checksums_offset_, checksums_size_,
        checksums_checksum_, meta_info_offset_, meta_info_size_, meta_info_checksum_,
        unique_key_index_offset_, unique_key_index_size_, unique_key_index_checksum_);
}

void PartInspector::Footer::read(ReadBuffer& reader) {
    readIntBinary(index_offset_, reader);
    readIntBinary(index_size_, reader);
    readIntBinary(index_checksum_, reader);
    readIntBinary(checksums_offset_, reader);
    readIntBinary(checksums_size_, reader);
    readIntBinary(checksums_checksum_, reader);
    readIntBinary(meta_info_offset_, reader);
    readIntBinary(meta_info_size_, reader);
    readIntBinary(meta_info_checksum_, reader);
    readIntBinary(unique_key_index_offset_, reader);
    readIntBinary(unique_key_index_offset_, reader);
    readIntBinary(unique_key_index_checksum_, reader);
    // readPODBinary(key_, reader);
}

String PartInspector::MetaInfo::info() const {
    return fmt::format("Version: {}, Deleted: {}, Bytes: {}, Rows: {}, Marks: {}, HintMutation: {}",
        static_cast<Int32>(version_), static_cast<Int32>(deleted_), bytes_on_disk_, rows_count_,
        marks_count_, hint_mutation_);
}

void PartInspector::MetaInfo::read(ReadBuffer& reader) {
    assertString("CHPT", reader);
    readIntBinary(version_, reader);
    readIntBinary(deleted_, reader);
    readVarUInt(bytes_on_disk_, reader);
    readVarUInt(rows_count_, reader);
    readVarUInt(marks_count_, reader);
    readVarUInt(hint_mutation_, reader);
}

String PartInspector::briefInfo() {
    return fmt::format("Meta: [{}], Footer: [{}]",
        metaInfo().info(), fileFooter().info());
}

String PartInspector::checksumsInfo() {
    std::stringstream ss;
    for (const auto& [file_name, checksum] : checksums().files) {
        ss << fmt::format("{}: Offset: {}, Size: {}, Deleted: {}, "
            "Mutation: {}, Hash: {}, Compressed: {}, "
            "UncompressedSize: {}, UncompressedHash: {}\n",
            file_name, checksum.file_offset, checksum.file_size,
            checksum.is_deleted, checksum.mutation, checksum.file_hash,
            checksum.is_compressed, checksum.uncompressed_size,
            checksum.uncompressed_hash);
    }
    return ss.str();
}

String PartInspector::marksInfo(const String& stream_name) {
    std::stringstream ss;

    if (stream_name.empty())
        throw Exception("stream_name is empty", ErrorCodes::BAD_ARGUMENTS);

    MergeTreeDataPartChecksums& part_checksums = checksums();
    auto iter = part_checksums.files.find(stream_name + MARKS_FILE_EXTENSION);
    if (iter == part_checksums.files.end()) {
        throw Exception(fmt::format("Didn't find marks file for \"{}\"", stream_name),
            ErrorCodes::BAD_ARGUMENTS);
    }

    size_t num_marks = iter->second.file_size / sizeof(MarkInCompressedFile);
    ss << "num marks: " << num_marks << ", mark file size: " << iter->second.file_size
        << ", each mark size: " << sizeof(MarkInCompressedFile) << ", marks detail: ";
    MarksInCompressedFile marks(num_marks);

    ReadBufferFromFileBase& raw_reader = rawReader();
    raw_reader.seek(iter->second.file_offset);
    raw_reader.readStrict(reinterpret_cast<char*>(marks.data()), iter->second.file_size);

    for (size_t i = 0; i < num_marks; ++i) {
        ss << "{" << i << ":" << marks[i].toString() << "},";
    }
    return ss.str();
}

String PartInspector::checkFiles(const String& stream_name) {
    std::stringstream ss;
    MergeTreeDataPartChecksums& part_checksums = checksums();
    for (const auto& [file_name, checksum] : part_checksums.files) {
        if (stream_name.empty() || startsWith(file_name, stream_name)) {
            ss << file_name << " " << checkFile(checksum) << "\n";
        }
    }

    return ss.str();
}

ReadBufferFromFileBase& PartInspector::rawReader() {
    if (reader_ == nullptr) {
        reader_ = fs_op_.read(path_);
    }

    return *reader_;
}

size_t PartInspector::fileSize() {
    if (!file_size_.has_value()) {
        file_size_ = fs_op_.getSize(path_);
    }

    return file_size_.value();
}

PartInspector::Footer& PartInspector::fileFooter() {
    if (footer_ == nullptr) {
        footer_ = std::make_unique<Footer>();

        ReadBufferFromFileBase& reader = rawReader();
        reader.seek(fileSize() - MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE);

        footer_->read(reader);
    }

    return *footer_;
}

PartInspector::MetaInfo& PartInspector::metaInfo() {
    if (meta_ == nullptr) {
        meta_ = std::make_unique<MetaInfo>();

        ReadBufferFromFileBase& reader = rawReader();
        reader.seek(fileFooter().meta_info_offset_);

        meta_->read(reader);
    }

    return *meta_;
}

MergeTreeDataPartChecksums& PartInspector::checksums() {
    if (checksums_ == nullptr) {
        checksums_ = std::make_unique<MergeTreeDataPartChecksums>();
        checksums_->storage_type = StorageType::ByteHDFS;

        ReadBufferFromFileBase& reader = rawReader();

        if (fileFooter().checksums_size_ != 0) {
            reader.seek(fileFooter().checksums_offset_);

            checksums_->read(reader);
        }
    }

    return *checksums_;
}

String PartInspector::checkFile(const MergeTreeDataPartChecksum& file_checksum) {
    std::stringstream ss;
    try {
        ReadBufferFromFileBase& reader = rawReader();
        reader.seek(file_checksum.file_offset);
        ss << "Offset " << file_checksum.file_offset << ", Size " << file_checksum.file_size << ", Check result: ";
        
        if (file_checksum.is_compressed) {
            LimitReadBuffer compressed(reader, file_checksum.file_size, false);
            HashingReadBuffer compressed_hashing(compressed);
            CompressedReadBuffer uncompressed(compressed_hashing);
            HashingReadBuffer uncompressed_hashing(uncompressed);
            uncompressed_hashing.ignoreAll();

            ss << " FileSize " << (compressed.count() == file_checksum.file_size);
            ss << ", FileHashing " << (compressed_hashing.getHash() == file_checksum.file_hash);
            ss << ", UncompressedSize " << (uncompressed.count() == file_checksum.uncompressed_size);
            ss << ", UncompressedHashing " << (uncompressed_hashing.getHash() == file_checksum.uncompressed_hash);
        } else {
            LimitReadBuffer compressed(reader, file_checksum.file_size, false);
            HashingReadBuffer compressed_hashing(compressed);
            compressed_hashing.ignoreAll();

            ss << ", FileSize " << (compressed.count() == file_checksum.file_size);
            ss << ", Hashing " << (compressed_hashing.getHash() == file_checksum.file_hash);
        }
    } catch (...) {
        return "Failed To Read";
    }

    return ss.str();
}

void ParallelInspectRunner::InspectTask::exec() {
    String msg = fmt::format("Path: {}", inspector_.path());
    if (type_ & BRIEF) {
        msg += (", " + inspector_.briefInfo());
    }
    if (type_ & MARKS) {
        msg += (", " + inspector_.marksInfo(stream_name_));
    }
    if (type_ & CHECKSUMS) {
        msg += (",\n" + inspector_.checksumsInfo());
    }
    if (type_ & CHECK_FILES) {
        msg += (",\n" + inspector_.checkFiles(stream_name_));
    }
    LOG_INFO(logger_, msg);
}

ParallelInspectRunner::TaskAllocator::TaskAllocator(const String& base_path, FSOp& fs_op, InspectTask::Type type,
    const String& stream_name, Poco::Logger* logger):
        base_path_(base_path), fs_op_(fs_op), type_(type), logger_(logger), stream_name_(stream_name) {
    if (fs_op_.isFile(base_path_)) {
        abs_paths_.push_back(base_path_);
    } else {
        collectPaths(base_path, fs_op_, &abs_paths_);
    }
}

std::unique_ptr<ParallelInspectRunner::InspectTask> ParallelInspectRunner::TaskAllocator::acquire() {
    std::lock_guard<std::mutex> lock(mu_);

    if (abs_paths_.empty()) {
        return nullptr;
    }

    String abs_path = abs_paths_.back();
    abs_paths_.pop_back();
    return std::make_unique<InspectTask>(abs_path, fs_op_, type_, stream_name_, logger_);
}

void ParallelInspectRunner::TaskAllocator::collectPaths(const String& base_path, FSOp& fs_op,
        std::vector<String>* file_abs_paths) {
    std::vector<String> file_names = fs_op.list(base_path);

    for (const String& file_name : file_names) {
        String path = std::filesystem::path(base_path) / file_name;

        if (fs_op.isFile(path)) {
            file_abs_paths->push_back(path);
        } else {
            collectPaths(path, fs_op, file_abs_paths);
        }
    }
}

ParallelInspectRunner::ParallelInspectRunner(const String& base_path, FSOp& fs_op,
    size_t worker_threads, InspectTask::Type type, const String& stream_name):
        task_allocator_(base_path, fs_op, type, stream_name, &Poco::Logger::get("ParallelInspectRunner")) {
    worker_pool_ = std::make_unique<ThreadPool>(worker_threads, worker_threads, worker_threads);

    for (size_t i = 0; i < worker_threads; ++i) {
        worker_pool_->scheduleOrThrowOnError(createExceptionHandledJob([&, this]() {
            while (true) {
                std::unique_ptr<InspectTask> task = task_allocator_.acquire();
                if (task == nullptr) {
                    return;
                }

                task->exec();
            }
        }, handler));
    }
}

void ParallelInspectRunner::wait() {
    worker_pool_->wait();
    worker_pool_ = nullptr;
    handler.throwIfException();
}

int parseAndRunInspectPartTask(const std::vector<String>& args) {
    po::options_description desc("part inspector");

    desc.add_options()
        ("help", po::bool_switch()->default_value(false), "Help")
        ("user", po::value<String>()->default_value("clickhouse"), "user when access hdfs")
        ("prefix", po::value<String>()->required(), "file system prefix, supported are "
            "https://host/bucket/prefix, http://host/bucket/prefix, hdfs://nnip:nnport/, "
            "cfs://nnip:nnport, nnproxy or empty for local file system")
        ("path", po::value<String>()->required(), "relative data path, can be a folder or just a file")
        ("threads", po::value<size_t>()->default_value(1), "threads to use")
        ("type", po::value<String>()->required(), "inspect type, can be brief, checksum, all, marks, check_files")
        ("stream", po::value<String>()->default_value("")->implicit_value(""), "inspect stream's mark content");

    po::variables_map options;
    po::store(po::command_line_parser(args).options(desc).run(), options);
    po::notify(options);

    if (options["help"].as<bool>()) {
        std::cout << desc << std::endl;
        return 0;
    }

    std::vector<String> required_field = {"prefix", "path", "type"};
    for (const String& field : required_field) {
        if (options.count(field) == 0) {
            std::cerr << "Missing required field " << field << "\n" << desc << std::endl;
            return 1;
        }
    }

    String prefix = options["prefix"].as<String>();
    String user = options["user"].as<String>();
    String path = options["path"].as<String>();
    size_t threads = options["threads"].as<size_t>();
    String task_type = options["type"].as<String>();
    String stream_name = options["stream"].as<String>();
    Poco::URI uri(prefix);

    if (threads == 0) {
        std::cerr << "Invalid thread num " << threads << std::endl;
        return 1;
    }

    std::unique_ptr<FSOp> fs = nullptr;
    if (prefix.empty()) {
        fs = std::make_unique<LocalFSOp>();
    } else {
        String uri_scheme = Poco::toLower(uri.getScheme());
        if (uri_scheme == "http" || uri_scheme == "https") {
            S3::URI s3_uri(uri);
            S3::S3Config s3_cfg(s3_uri.endpoint, s3_uri.region, s3_uri.bucket, "", "",
                s3_uri.key);
            s3_cfg.collectCredentialsFromEnv();

            fs = std::make_unique<S3FSOp>(s3_cfg);
        } else {
            HDFSConnectionParams hdfs_params;
            if (uri_scheme.empty()) {
                hdfs_params = HDFSConnectionParams(HDFSConnectionParams::CONN_NNPROXY,
                    user, prefix);
            } else {
                HDFSConnectionParams::HDFSConnectionType type = HDFSConnectionParams::CONN_DUMMY;
                if (uri_scheme == "cfs") {
                    type = HDFSConnectionParams::CONN_CFS;
                } else if (uri_scheme == "hdfs") {
                    type = HDFSConnectionParams::CONN_HDFS;
                } else {
                    std::cerr << "Failed to parse uri prefix " << prefix << std::endl;
                    return 1;
                }

                String host = uri.getHost();
                int port = uri.getPort() == 0 ? 65212 : uri.getPort();
                hdfs_params = HDFSConnectionParams(type, user, {{host, port}});
            }

            fs = std::make_unique<HDFSFSOp>(hdfs_params);
        }
    }

    ParallelInspectRunner::InspectTask::Type tsk_type = ParallelInspectRunner::InspectTask::ALL;
    task_type = Poco::toLower(task_type);
    if (task_type == "brief") {
        tsk_type = ParallelInspectRunner::InspectTask::BRIEF;
    } else if (task_type == "checksum") {
        tsk_type = ParallelInspectRunner::InspectTask::CHECKSUMS;
    } else if (task_type == "all") {
        tsk_type = ParallelInspectRunner::InspectTask::ALL;
    } else if (task_type == "marks") {
        tsk_type = ParallelInspectRunner::InspectTask::MARKS_DETAIL;
    } else if (task_type == "check_files") {
        tsk_type = ParallelInspectRunner::InspectTask::CHECK_FILES;
    } else {
        std::cerr << "Unknown task type " << task_type << ", supported are brief/checksum/marks/all" << std::endl;
        return 1;
    }

    ParallelInspectRunner runner(path, *fs, threads, tsk_type, stream_name);

    runner.wait();

    return 0;
}

}
