#pragma once

#include "Common/config.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#    include <common/types.h>

#    define TOKEN_CONCAT(x, y) x##y
#    define TOKEN_CONCAT_FWD(x, y) TOKEN_CONCAT(x, y)

#    define LOG_TIMEUSAGE() \
        const auto TOKEN_CONCAT_FWD(start_time_, __LINE__) = std::chrono::steady_clock::now(); \
        String function_name = __FUNCTION__; \
        SCOPE_EXIT(LOG_DEBUG( \
            log, \
            "{} elapsed: {}ms", \
            function_name, \
            std::chrono::duration_cast<std::chrono::milliseconds>( \
                std::chrono::steady_clock::now() - TOKEN_CONCAT_FWD(start_time_, __LINE__)) \
                .count()))

#    define SET_JNI_CONTEXT() \
        jni::JniQueryIdSwither TOKEN_CONCAT_FWD(jni_query_id_switcher_, __LINE__)(CurrentThread::getQueryId().toString())

namespace DB
{
namespace ErrorCodes
{
    extern const int JNI_ERROR;
}
namespace jni
{
    // This thread_local defined in contrib/java-extensions/cpp/src/JNIHelpers.cpp
    extern thread_local String tls_query_id;

    class JniQueryIdSwither
    {
    public:
        explicit JniQueryIdSwither(const String & current) : previous(tls_query_id) { tls_query_id = current; }
        ~JniQueryIdSwither() { tls_query_id = previous; }

    private:
        String previous;
    };
}
}
#endif

