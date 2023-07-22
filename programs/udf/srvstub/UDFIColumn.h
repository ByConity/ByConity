#pragma once

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <linux/limits.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include <Core/Types.h>
#include <Common/PODArray_fwd.h>
#include <Functions/UserDefined/FormatPath.h>
#include <Functions/UserDefined/Proto.h>

class UDFIColumn {
public:
    DB::TypeIndex type;

    UDFIColumn(DB::TypeIndex type_, uint64_t prefix_, uint16_t idx)
    : type(type_), prefix(prefix_)
    {
        data = open_fd(idx, calcLeftPadding(getElementSize()), &len);
    }

    virtual ~UDFIColumn()
    {
        char *start;

        start = static_cast<char*>(data) - calcLeftPadding(getElementSize());
        munmap(start, len);
    }

    void* getData() const {
        return data;
    }

protected:
    [[noreturn]] void throwError(const char *key, const char* name) const {
        char buf[PATH_MAX];
        int t;

        t = static_cast<std::underlying_type<DB::TypeIndex>::type>(type);
        snprintf(buf, PATH_MAX, "UDFSrv %s failed: %s, file %s, type:%d", key,
                 strerror(errno), name, t);
        throw std::runtime_error(buf);
    }

    int openFd(uint16_t idx) const {
        char filename[PATH_MAX];

        shm_file_fmt(filename, prefix, idx);
        return open(filename, O_RDWR);
    }

    void *open_fd(uint16_t idx, int padding, size_t *length, int *fd = nullptr) const {
        /* fd == nullptr means the file was created by the client side */
        int flag = fd ? O_RDWR | O_CREAT : O_RDWR;
        char filename[PATH_MAX];
        struct stat sb;
        void *pMap;
        int fdIn;

        shm_file_fmt(filename, prefix, idx);

        if (!length)
            throw std::runtime_error("zero length");

        fdIn = shm_open(filename, flag, __S_IREAD | __S_IWRITE);
        if (fdIn < 0)
            throwError("open", filename);

        if (fd) {
            if (ftruncate(fdIn, *length) == -1)
                throwError("ftruncate", filename);
        } else {
            if ((fstat(fdIn, &sb)) == -1)
                throwError("fstat", filename);
            *length = sb.st_size;
        }

        pMap = mmap(nullptr, *length, PROT_READ | PROT_WRITE, MAP_SHARED, fdIn, 0);

        if (fd)
            *fd = fdIn;
        else
            close(fdIn);

        if (pMap == MAP_FAILED)
            throwError("mmap", filename);

        return static_cast<char*>(pMap) + padding;
    }

    static constexpr size_t calcLeftPadding(size_t size) {
        size_t pad_left = 16;

        return DB::integerRoundUp(DB::integerRoundUp(pad_left, size), 16);
    }

    [[noreturn]] static void throw_enlarge_error(int fd, const char *key, size_t before, size_t after) {
        char buf[PATH_MAX];

        snprintf(buf, PATH_MAX,
                 "UDFSrv %s when enlarging failed: %s, fd:%d before:%zu after:%zu",
                 key, strerror(errno), fd, before, after);

        throw std::runtime_error(buf);
    }

    static void *enlarge(int fd, void *buf, size_t before, size_t after) {
        if (ftruncate(fd, after) == -1)
            throw_enlarge_error(fd, "ftruncate", before, after);

        buf = mremap(buf, before, after, MREMAP_MAYMOVE);
        if (buf == MAP_FAILED)
            throw_enlarge_error(fd, "mremap", before, after);

        return buf;
    }

private:
    bool isNested() const
    {
        switch (type) {
                case DB::TypeIndex::Nullable:
                return true;
                case DB::TypeIndex::Array:
                return true;
                case DB::TypeIndex::Set:
                case DB::TypeIndex::Interval:
                case DB::TypeIndex::Function:
                case DB::TypeIndex::AggregateFunction:
                case DB::TypeIndex::Map:
                case DB::TypeIndex::Enum8:
                case DB::TypeIndex::Enum16:
                case DB::TypeIndex::LowCardinality:
                throw std::runtime_error("unsupported UDF type");
            default:
                return false;
        }
    }

    size_t getElementSize() const {
        switch (type) {
                case DB::TypeIndex::UInt8:
                case DB::TypeIndex::Int8:
                case DB::TypeIndex::FixedString:
                case DB::TypeIndex::Nullable:
                return 1;
                case DB::TypeIndex::UInt16:
                case DB::TypeIndex::Int16:
                case DB::TypeIndex::Date:
                return 2;
                case DB::TypeIndex::UInt32:
                case DB::TypeIndex::Int32:
                case DB::TypeIndex::Float32:
                case DB::TypeIndex::DateTime:
                case DB::TypeIndex::Decimal32:
                return 4;
                case DB::TypeIndex::String:
                case DB::TypeIndex::UInt64:
                case DB::TypeIndex::Int64:
                case DB::TypeIndex::Float64:
                case DB::TypeIndex::Decimal64:
                case DB::TypeIndex::Array:
                return 8;
                case DB::TypeIndex::UUID:
                case DB::TypeIndex::UInt128:
                case DB::TypeIndex::Int128:
                case DB::TypeIndex::Decimal128:
                return 16;
                case DB::TypeIndex::Tuple:
                case DB::TypeIndex::Set:
                case DB::TypeIndex::Interval:
                case DB::TypeIndex::Function:
                case DB::TypeIndex::AggregateFunction:
                case DB::TypeIndex::Map:
                case DB::TypeIndex::Enum8:
                case DB::TypeIndex::Enum16:
                case DB::TypeIndex::LowCardinality:
            default:
                throw std::runtime_error("unsupported UDF type");
                break;
        }
    }

protected:
    bool isNestedColumn;
    void *data;

private:
    uint64_t prefix;
    size_t len;
};
