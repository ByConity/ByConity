#pragma once

#include <cstddef>
#include <ostream>
#include <string>
#include <common/logger_useful.h>

#ifdef __AVX2__
#include <Common/JSONParsers/jsonb/JSONBParserSimd.h>
#else
#include <Common/JSONParsers/jsonb/JSONBParser.h>
#endif

namespace DB {

struct JsonBinaryValue {
    static const int MAX_LENGTH = (1 << 30);

    // default nullprt and size 0 for invalid or NULL value
    const char* ptr = nullptr;
    size_t len = 0;
    JsonbParser parser;
    Poco::Logger * logger = &Poco::Logger::get("JsonBinaryValue");

    JsonBinaryValue() : ptr(nullptr), len(0) {}
    JsonBinaryValue(char* ptr_, int len_) {
        static_cast<void>(fromJsonString(const_cast<const char*>(ptr_), len_));
    }
    explicit JsonBinaryValue(const std::string& s) {
        static_cast<void>(fromJsonString(s.c_str(), s.length()));
    }
    
    JsonBinaryValue(const char* ptr_, int len_) { static_cast<void>(fromJsonString(ptr_, len_)); }

    const char* value() const { return ptr; }

    size_t size() const { return len; }

    void replace(char* ptr_, int len_) {
        this->ptr = ptr_;
        this->len = len_;
    }

    bool operator==(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }
    // !=
    bool ne(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }
    // <=
    bool le(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }
    // >=
    bool ge(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }
    // <
    bool lt(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }
    // >
    bool gt(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }

    bool operator!=(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }

    bool operator<=(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }

    bool operator>=(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }

    bool operator<(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }

    bool operator>(const JsonBinaryValue&  /*other*/) const {
        LOG_FATAL(logger, "comparing between JsonBinaryValue is not supported");
        __builtin_unreachable();
    }

    bool fromJsonString(const char* s, int len);

    std::string toJsonString() const;

    // struct HashOfJsonBinaryValue {
    //     size_t operator()(const JsonBinaryValue& v) const {
    //         return HashUtil::hash(v.ptr, v.len, 0);
    //     }
    // };
};

// This function must be called 'hash_value' to be picked up by boost.
// inline std::size_t hash_value(const JsonBinaryValue& v) {
//     return HashUtil::hash(v.ptr, v.len, 0);
// }

std::ostream& operator<<(std::ostream& os, const JsonBinaryValue& json_value);

std::size_t operator-(const JsonBinaryValue& v1, const JsonBinaryValue& v2);

} // namespace DB
