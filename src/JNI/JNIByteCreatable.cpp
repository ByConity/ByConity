#include "JNIByteCreatable.h"

#include "JNIHelper.h"

#include <cassert>
#include <iostream>

namespace DB
{
JNIByteCreatable::JNIByteCreatable(const std::string & full_classname, const std::string & pb_message)
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();

    jni_cls = env->FindClass(full_classname.c_str());
    if (!jni_cls)
    {
        fprintf(stderr, "could not find class %s\n", full_classname.c_str());
        return;
    }

    std::string factory_method_signature = "([B)L" + full_classname + ";";

    jmethodID factory_method = env->GetStaticMethodID(jni_cls, "create", factory_method_signature.c_str());
    if (!factory_method)
    {
        fprintf(stderr, "could not find factory method 'create' from class %s\n", full_classname.c_str());
        return;
    }

    /// build byte array
    const char * chars = pb_message.c_str();
    int array_length = pb_message.length();
    jbyteArray jbyte_array = env->NewByteArray(array_length);
    env->SetByteArrayRegion(jbyte_array, 0, array_length, reinterpret_cast<const jbyte *>(chars));

    /// invoke factory method
    jni_obj = env->CallStaticObjectMethod(jni_cls, factory_method, jbyte_array);
    if (env->ExceptionCheck())
    {
        env->ExceptionDescribe();
    }
    env->DeleteLocalRef(jbyte_array);
}

JNIByteCreatable::~JNIByteCreatable()
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    env->DeleteLocalRef(jni_obj);
    env->DeleteLocalRef(jni_cls);
}

}
