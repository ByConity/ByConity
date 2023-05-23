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
    method_get_table = env->GetMethodID(jni_cls, "getTable", "()[B");
    assert(method_get_table != nullptr);

    method_get_all_partitions = env->GetMethodID(jni_cls, "getPartitionPaths", "()[B");
    assert(method_get_all_partitions != nullptr);

    method_get_files_in_partition = env->GetMethodID(jni_cls, "getFilesInPartition", "(Ljava/lang/String;)[B");
    assert(method_get_files_in_partition != nullptr);
}

std::string JNIMetaClient::getTable()
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    jbyteArray jreturn = static_cast<jbyteArray>(env->CallObjectMethod(jni_obj, method_get_table));
    assert(jreturn != nullptr);
    jbyte* bytes = env->GetByteArrayElements(jreturn, nullptr);
    jsize length = env->GetArrayLength(jreturn);
    std::string result(reinterpret_cast<char*>(bytes), length);
    env->ReleaseByteArrayElements(jreturn, bytes, JNI_ABORT);
    return result;
}

std::string JNIMetaClient::getPartitionPaths()
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    jbyteArray jreturn = static_cast<jbyteArray>(env->CallObjectMethod(jni_obj, method_get_all_partitions));
    assert(jreturn != nullptr);
    jbyte* bytes = env->GetByteArrayElements(jreturn, nullptr);
    jsize length = env->GetArrayLength(jreturn);
    std::string result(reinterpret_cast<char*>(bytes), length);
    env->ReleaseByteArrayElements(jreturn, bytes, JNI_ABORT);
    return result;
}

std::string JNIMetaClient::getFilesInPartition(const std::string &partition_path)
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    jstring jstr_partition_path = env->NewStringUTF(partition_path.c_str());
    jbyteArray jreturn
        = static_cast<jbyteArray>(env->CallObjectMethod(jni_obj, method_get_files_in_partition, jstr_partition_path));
    assert(jreturn != nullptr);
    jbyte * bytes = env->GetByteArrayElements(jreturn, nullptr);
    jsize length = env->GetArrayLength(jreturn);
    std::string result(reinterpret_cast<char *>(bytes), length);
    env->ReleaseByteArrayElements(jreturn, bytes, JNI_ABORT);
    return result;
}

}
