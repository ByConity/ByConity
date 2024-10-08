#include <Common/Logger.h>
#include <iostream>
#include <mutex>
#include <memory>
#include <filesystem>
#include <fmt/format.h>
#include <Core/Types.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>
// #include <Encryption/AesEncrypt.h>
#include <city.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

template <> struct fmt::formatter<CityHash_v1_0_2::uint128> {
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const CityHash_v1_0_2::uint128& p, FormatContext& ctx) -> decltype(ctx.out()) {
        return format_to(ctx.out(), "{}-{}", p.first, p.second);
    }
};

namespace DB {

class FSOp {
public:
    virtual ~FSOp() {}

    virtual std::unique_ptr<ReadBufferFromFileBase> read(const String& path) = 0;
    virtual size_t getSize(const String& path) = 0;
    virtual bool isFile(const String& path) = 0;
    virtual std::vector<String> list(const String& path) = 0;
};

class PartInspector {
public:
    using uint128 = CityHash_v1_0_2::uint128;

    PartInspector(const String& path, FSOp& fs):
        path_(path), fs_op_(fs) {}

    struct Footer {
        Footer(): index_offset_(0), index_size_(0), index_checksum_(),
            checksums_offset_(0), checksums_size_(0), checksums_checksum_(),
            meta_info_offset_(0), meta_info_size_(0), meta_info_checksum_(),
            unique_key_index_offset_(0), unique_key_index_size_(0), unique_key_index_checksum_() {}

        String info() const;

        void read(ReadBuffer& reader);

        off_t index_offset_;
        size_t index_size_;
        uint128 index_checksum_;
        off_t checksums_offset_;
        size_t checksums_size_;
        uint128 checksums_checksum_;
        off_t meta_info_offset_;
        size_t meta_info_size_;
        uint128 meta_info_checksum_;
        off_t unique_key_index_offset_;
        size_t unique_key_index_size_;
        uint128 unique_key_index_checksum_;
        // AesEncrypt::AesKeyByteArray key_;
    };

    struct MetaInfo {
        MetaInfo(): version_(0), deleted_(0), bytes_on_disk_(0), rows_count_(0),
            marks_count_(0), hint_mutation_(0) {}

        String info() const;

        void read(ReadBuffer& reader);

        UInt8 version_;
        UInt8 deleted_;
        UInt64 bytes_on_disk_;
        size_t rows_count_;
        size_t marks_count_;
        Int64 hint_mutation_;
    };

    const String& path() const {
        return path_;
    }

    String briefInfo();

    String checksumsInfo();

    String marksInfo(const String& stream_name);

    String checkFiles(const String& stream_name);

private:
    ReadBufferFromFileBase& rawReader();

    size_t fileSize();

    Footer& fileFooter();

    MetaInfo& metaInfo();

    MergeTreeDataPartChecksums& checksums();

    String checkFile(const MergeTreeDataPartChecksum& file_checksum);

    String path_;
    FSOp& fs_op_;

    std::optional<size_t> file_size_;
    std::unique_ptr<ReadBufferFromFileBase> reader_;
    std::unique_ptr<Footer> footer_;
    std::unique_ptr<MetaInfo> meta_;
    std::unique_ptr<MergeTreeDataPartChecksums> checksums_; 
};

class ParallelInspectRunner {
public:
    class InspectTask {
    public:
        enum Type {
            BRIEF = 1,
            CHECKSUMS = 1 << 1,
            MARKS = 1 << 2,
            CHECK_FILES = 1 << 3,
            MARKS_DETAIL = BRIEF | MARKS,
            ALL = BRIEF | CHECKSUMS,
        };

        InspectTask(const String& path, FSOp& fs, Type type, const String& stream_name, LoggerPtr logger):
            type_(type), logger_(logger), stream_name_(stream_name), inspector_(path, fs) {}

        void exec();

    private:
        Type type_;
        LoggerPtr logger_;
        String stream_name_;
        PartInspector inspector_;
    };

    class TaskAllocator {
    public:
        TaskAllocator(const String& base_path, FSOp& fs_op, InspectTask::Type type,
            const String& stream_name, LoggerPtr logger);

        std::unique_ptr<InspectTask> acquire();

        static void collectPaths(const String& base_path, FSOp& fs_op,
            std::vector<String>* file_abs_paths);
    
    private:
        const String base_path_;
        FSOp& fs_op_;

        InspectTask::Type type_;

        LoggerPtr logger_;

        String stream_name_;

        std::mutex mu_;
        std::vector<String> abs_paths_;
    };

    ParallelInspectRunner(const String& base_path, FSOp& fs_op,
        size_t worker_threads, InspectTask::Type type, const String& stream_name);

    void wait();

private:
    TaskAllocator task_allocator_;

    ExceptionHandler handler;
    std::unique_ptr<ThreadPool> worker_pool_;
};

int parseAndRunInspectPartTask(const std::vector<String>& args);

}
