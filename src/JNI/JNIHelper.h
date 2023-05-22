#pragma once

#include <map>
#include <string>
#include "jni.h"

namespace DB
{


class JNIHelper final {
public:
    static JNIHelper & instance();

    /// get the JNIEnv for the given thread
    /// if no JVM exists, the global one will be created
    /// CLASSPATH environment variable
    /// similar to hadoop-hdfs/src/main/native/libhdfs/jni_helper.h
    JNIEnv * getJNIEnv() { return cached_env; }

private:
    void init();
    JNIHelper() { init(); }
    inline static thread_local JNIEnv * cached_env;

public:
    /// common jclass
    jclass object_class;
    jclass object_array_class;
    jclass string_class;
    jclass throwable_class;
    jclass arrays_class;
    jclass list_class;
    jclass hashmap_class;
};

}
