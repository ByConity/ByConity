#include "JNIMetaClient.h"

#include "JNIByteCreatable.h"
#include "JNIHelper.h"

#include <cassert>
#include <iostream>

namespace DB
{
JNIMetaClient::JNIMetaClient(const std::string &full_classname, const std::string & pb_message)
: JNIByteCreatable(full_classname, pb_message)
{
    JNIEnv *env = JNIHelper::instance().getJNIEnv();
    method_get_table = env->GetMethodID(jni_cls, "getTable", "(Ljava/lang/String;)[B");
    assert(method_get_table);
}

std::string JNIMetaClient::getTable(const std::string & tablename)
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    jstring jstr = env->NewStringUTF(tablename.c_str());
    jbyteArray jreturn = static_cast<jbyteArray>(env->CallObjectMethod(jni_obj, method_get_table, jstr));
    assert(jreturn != nullptr);
    jbyte* bytes = env->GetByteArrayElements(jreturn, nullptr);
    jsize length = env->GetArrayLength(jreturn);

    size_t cpp_length = static_cast<size_t>(length);
    std::string result;
    result.resize(cpp_length);
    for (size_t i = 0; i < cpp_length; i++)
    {
        result[i] = bytes[i];
    }

    env->ReleaseByteArrayElements(jreturn, bytes, JNI_ABORT);
    return result;
}
}
