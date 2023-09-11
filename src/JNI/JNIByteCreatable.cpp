#include "JNIByteCreatable.h"

#include "JNIHelper.h"

#include <cassert>
#include <iostream>

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{
JNIByteCreatable::JNIByteCreatable(const std::string & full_classname, const std::string & pb_message)
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    CHECK_JNI_EXCEPTION(jni::findClass(env, &jni_cls, full_classname.c_str()), "Cannot find class " + full_classname);
    std::string factory_method_signature = "([B)L" + full_classname + ";";
    jmethodID factory_method;
    CHECK_JNI_EXCEPTION(jni::findMethodId(env, &factory_method, jni_cls, "create", factory_method_signature.c_str(), true), "Cannot find static create factory method");

    /// build byte array
    const char * chars = pb_message.c_str();
    int array_length = pb_message.length();
    jbyteArray jbyte_array = env->NewByteArray(array_length);
    JNILocalRefWrapper wrapper(env, jbyte_array);
    env->SetByteArrayRegion(jbyte_array, 0, array_length, reinterpret_cast<const jbyte *>(chars));

    /// invoke factory method
    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeStaticMethod(env, &return_val, jni_cls, factory_method, factory_method_signature.c_str(), jbyte_array));
    jni_obj = return_val.l;
    assert(jni_obj != nullptr);
}

JNIByteCreatable::~JNIByteCreatable()
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    env->DeleteLocalRef(jni_obj);
    env->DeleteLocalRef(jni_cls);
}

void JNIByteCreatable::registerMethod(JNIEnv * env, const std::string & method_name, const std::string & method_signature)
{
    auto it = methods.find(method_name);
    if (it != methods.end())
        abort(); /// duplicate method, logical error

    jmethodID method_id;
    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &method_id, jni_cls, method_name.c_str(), method_signature.c_str(), false),
        "Unable to find method " + method_name);
    methods[method_name] = Method{method_signature, method_id};
}
}
