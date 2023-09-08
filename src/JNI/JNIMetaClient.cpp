#include "JNIMetaClient.h"

#include "JNIByteCreatable.h"
#include "JNIHelper.h"

#include <cassert>
#include <iostream>

namespace DB
{
JNIMetaClient::JNIMetaClient(const std::string & full_classname, const std::string & pb_message)
    : JNIByteCreatable(full_classname, pb_message)
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    registerMethod(env, "getTable", "()[B");
    registerMethod(env, "getPartitionPaths", "()[B");
    registerMethod(env, "getFilesInPartition", "(Ljava/lang/String;)[B");
}

std::string JNIMetaClient::getTable()
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    const auto & method = getMethod("getTable");
    jvalue return_val;
    THROW_JNI_EXCEPTION(
        env, jni::invokeObjectMethod(env, &return_val, jni_obj, method.method_id, method.signature.c_str()));
    jbyteArray res = static_cast<jbyteArray>(return_val.l);
    JNILocalRefWrapper wrapper(env, res);
    return jni::jbyteArrayToStr(env, res);
}

std::string JNIMetaClient::getPartitionPaths()
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    const auto & method = getMethod("getPartitionPaths");
    jvalue return_val;
    THROW_JNI_EXCEPTION(
        env, jni::invokeObjectMethod(env, &return_val, jni_obj, method.method_id, method.signature.c_str()));
    jbyteArray res = static_cast<jbyteArray>(return_val.l);
    JNILocalRefWrapper wrapper(env, res);
    return jni::jbyteArrayToStr(env, res);
}

std::string JNIMetaClient::getFilesInPartition(const std::string & partition_path)
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    const auto & method = getMethod("getFilesInPartition");
    jstring jstr_partition_path = env->NewStringUTF(partition_path.c_str());
    jvalue return_val;
    THROW_JNI_EXCEPTION(
        env, jni::invokeObjectMethod(env, &return_val, jni_obj, method.method_id, method.signature.c_str(), jstr_partition_path));
    jbyteArray res = static_cast<jbyteArray>(return_val.l);
    JNILocalRefWrapper wrapper(env, res);
    return jni::jbyteArrayToStr(env, res);
}

}
