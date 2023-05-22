#pragma once

#include "jni.h"

#include <string>

namespace DB
{
// General interface for byte creatable JNI class
// a factory method 'create(byte[])' should be implemented for each Java class
// single thread
class JNIByteCreatable {
public:
    JNIByteCreatable(const std::string &full_classname, const std::string & pb_message);

    virtual ~JNIByteCreatable();

protected:
    jclass jni_cls;
    jobject jni_obj;
};
}
