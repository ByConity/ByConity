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

/// Call env->deleteLocalRef on distruction
class JNILocalRefWrapper
{
public:
    JNILocalRefWrapper(JNIEnv * env_, jobject obj_) : env(env_), local_ref(obj_) { }

    ~JNILocalRefWrapper()
    {
        if (local_ref != nullptr)
            env->DeleteLocalRef(local_ref);
    }

    JNILocalRefWrapper(const JNILocalRefWrapper &) = delete;
    JNILocalRefWrapper & operator=(const JNILocalRefWrapper &) = delete;
    JNILocalRefWrapper(JNILocalRefWrapper &&) = delete;
    void operator=(JNILocalRefWrapper &&) = delete;

private:
    JNIEnv * env;
    jobject local_ref;
};

namespace jni
{
    jthrowable findClass(JNIEnv *env, jclass * out, const char * className);
    jthrowable findMethodId(JNIEnv * env, jmethodID * out, jclass cls, const char * methodName, const char * methodSiganture, bool static_method);

    jthrowable invokeObjectMethod(JNIEnv * env, jvalue * retval, jobject instObj, jmethodID mid, const char * methSignature, ...);
    jthrowable invokeStaticMethod(JNIEnv * env, jvalue * retval, jclass cls, jmethodID mid, const char * methSignature, ...);
    std::string jstringToStr(JNIEnv * env, jstring jstr);
    std::string jbyteArrayToStr(JNIEnv * env, jbyteArray obj);
    std::string getExceptionSummary(JNIEnv *env, jthrowable jthr);
}

#define CHECK_JNI_EXCEPTION(jthr, errorMsg) \
    do \
    { \
        if ((jthr) != nullptr) \
        { \
            throw std::runtime_error(errorMsg); \
        } \
    } while (false)

#define THROW_JNI_EXCEPTION(env, jthr) \
    do \
    { \
        if ((jthr) != nullptr) \
        { \
            throw std::runtime_error(jni::getExceptionSummary((env), (jthr))); \
        } \
    } while (false)

}
