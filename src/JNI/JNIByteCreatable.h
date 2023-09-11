#pragma once

#include "jni.h"

#include <string>
#include <unordered_map>

namespace DB
{
// General interface for byte creatable JNI class
// a factory method 'create(byte[])' should be implemented for each Java class
// single thread
class JNIByteCreatable {
public:
    JNIByteCreatable(const std::string & full_classname, const std::string & pb_message);

    virtual ~JNIByteCreatable();

protected:
    struct Method
    {
        std::string signature;
        jmethodID method_id;
    };
    void registerMethod(JNIEnv * env, const std::string & method_name, const std::string & method_signature);
    const Method & getMethod(const std::string & method_name) { return methods[method_name]; }

    jclass jni_cls;
    jobject jni_obj;

    std::unordered_map<std::string, Method> methods;
};
}
