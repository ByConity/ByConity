#pragma once

#include <cstdint>
#include <Core/Types.h>

#include "UDFIColumn.h"

class UDFColumnString final: public UDFIColumn
{
public:
    UDFColumnString(DB::TypeIndex type_idx_, uint64_t prefix_,
                    uint32_t offset_fd_, uint32_t chars_fd_,
                    uint64_t row_cnts, bool isOut) :
        UDFIColumn(type_idx_, prefix_, offset_fd_) {
        if (!isOut) {
            chars = static_cast<char *>(open_fd(chars_fd_, 0, &len, nullptr));
            return;
        }

        len = getInitialSize(row_cnts);
        chars = static_cast<char *>(open_fd(chars_fd_, 0, &len, &fd));
    }

    ~UDFColumnString() override {
        munmap(chars, len);
        if (fd != -1)
            close(fd);
    }

    static constexpr size_t getInitialSize(size_t row_cnts) {
        /* page aligned size */
        return (row_cnts * 64 + 4095) & ~4095;
    }

    constexpr size_t getPadding() const {
        return calcLeftPadding(sizeof(char));
    }

    /* including padding area */
    char *getChars() const {
        return chars + getPadding();
    }

    uint64_t *getOffset() const {
        return static_cast<uint64_t *>(getData());
    }

    size_t enlarge(size_t expected_size) {
        size_t after = len;

        while (true) {
            after *= 2;
            if (after >= expected_size)
                break;
        }

        chars = static_cast<char *>(UDFIColumn::enlarge(fd, chars, len, after));
        len = after;

        return after;
    }

    /* write total length into left padding area */
    void setCharsLength(uint64_t size) {
        uint64_t *p = reinterpret_cast<uint64_t *>(chars);

        *p = size + getPadding();
    }

    size_t getLength() const {
        return len;
    }

private:
    char *chars;
    size_t len;
    int fd = -1;
};
