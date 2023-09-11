#pragma clang diagnostic ignored "-Wunused-macros"

#include "JNIHelper.h"

#include <cassert>
#include <mutex>
#include <string>
#include <vector>

static constexpr auto CLASSPATH = "CLASSPATH";
static constexpr auto OPT_CLASSPATH = "-Djava.class.path=";
static constexpr auto JVM_ARGS = "LIBHDFS_OPTS";

/** The Native return types that methods could return */
#define JVOID         'V'
#define JOBJECT       'L'
#define JARRAYOBJECT  '['
#define JBOOLEAN      'Z'
#define JBYTE         'B'
#define JCHAR         'C'
#define JSHORT        'S'
#define JINT          'I'
#define JLONG         'J'
#define JFLOAT        'F'
#define JDOUBLE       'D'

// bthread?
static std::mutex jni_global_mutex;

/// global mutex must be acquired to call this function
static JNIEnv* getGlobalJNIEnv()
{
    static constexpr auto VM_BUF_LENGTH = 1;
    JavaVM* vm_buf[VM_BUF_LENGTH];
    jint num_vms = 0;
    JavaVM *vm;
    JNIEnv *jni_env;
    jint rv = JNI_GetCreatedJavaVMs(&vm_buf[0], VM_BUF_LENGTH, &num_vms);
    if (rv != 0)
    {
        std::fprintf(stderr, "JNI_GetCreatedJavaVMs failed with error: %d\n", rv);
        return nullptr;
    }

    if (num_vms == 0)
    {
        std::vector<std::string> options = {"-Djdk.lang.processReaperUseDefaultStackSize=true", "-Xrs"};

        char * class_path = getenv(CLASSPATH);
        if (class_path != nullptr)
        {
            std::string opt_class_path = OPT_CLASSPATH;
            opt_class_path.append(class_path);
            options.push_back(std::move(opt_class_path));
        }
        else
        {
            fprintf(stderr, "Environment variable CLASSPATH not set!\n");
            return nullptr;
        }

        char * jvm_args = getenv(JVM_ARGS);
        if (jvm_args != nullptr)
        {
            std::string opt_jvm_args = jvm_args;
            std::string_view view(opt_jvm_args);
            size_t start_pos = 0;
            while (start_pos != std::string::npos)
            {
                size_t end_pos = view.find(' ', start_pos);
                options.push_back(opt_jvm_args.substr(start_pos, end_pos - start_pos));
                start_pos = (end_pos == std::string::npos) ? end_pos : end_pos + 1;
            }
        }

        JavaVMInitArgs vm_args;
        JavaVMOption vm_options[options.size()];
        for (size_t i = 0; i < options.size(); ++i)
        {
            vm_options[i].optionString = options[i].data();
        }

        vm_args.version = JNI_VERSION_1_8;
        vm_args.options = vm_options;
        vm_args.nOptions = options.size();
        vm_args.ignoreUnrecognized = JNI_TRUE;

        rv = JNI_CreateJavaVM(&vm, reinterpret_cast<void**>(&jni_env), &vm_args);
        if (rv != 0)
        {
            std::fprintf(stderr, "JNI_CreateJavaVM failed with error: %d\n", rv);
            return nullptr;
        }
    }
    else
    {
        vm = vm_buf[0];
        rv = vm->AttachCurrentThread(reinterpret_cast<void**>(&jni_env), nullptr);
        if (rv != 0)
        {
            fprintf(stderr, "AttachCurrentThread failed with error: %d\n", rv);
            return nullptr;
        }
    }

    return jni_env;
}

namespace DB
{

JNIHelper & JNIHelper::instance()
{
    if (cached_env == nullptr)
    {
        /// just use global mutex here
        std::lock_guard lock(jni_global_mutex);
        cached_env = getGlobalJNIEnv();
    }

    assert(cached_env != nullptr);
    static JNIHelper helper;
    return helper;
}

void JNIHelper::init()
{
    jni::findClass(cached_env, &object_class, "java/lang/Object");
    jni::findClass(cached_env, &object_array_class, "[Ljava/lang/Object;");
    jni::findClass(cached_env, &string_class, "java/lang/String");
    jni::findClass(cached_env, &throwable_class, "java/lang/Throwable");
    jni::findClass(cached_env, &arrays_class, "java/util/Arrays");
    jni::findClass(cached_env, &arrays_class, "java/util/List");
    jni::findClass(cached_env, &hashmap_class, "java/util/HashMap");
}

namespace jni
{
jthrowable getPendingExceptionAndClear(JNIEnv *env)
{
    jthrowable jthr = env->ExceptionOccurred();
    if (jthr == nullptr)
        return jthr;

    env->ExceptionDescribe();
    env->ExceptionClear();
    return jthr;
}

jthrowable findClass(JNIEnv *env, jclass * out, const char * className)
{
    jthrowable jthr = nullptr;
    jclass global_class = nullptr;
    jclass local_class = nullptr;
    do
    {
        local_class = env->FindClass(className);
        if (!local_class)
        {
            jthr = getPendingExceptionAndClear(env);
            break;
        }
        global_class = static_cast<jclass>(env->NewGlobalRef(local_class));
        if (!global_class)
        {
            jthr = getPendingExceptionAndClear(env);
            break;
        }
        *out = global_class;
        jthr = nullptr;
    } while (false);

    env->DeleteLocalRef(local_class);
    if (jthr && global_class)
    {
        env->DeleteGlobalRef(global_class);
    }
    assert(*out != nullptr);
    return jthr;
}

jthrowable findMethodId(JNIEnv * env, jmethodID * out, jclass cls, const char * methodName, const char * methodSiganture, bool static_method)
{
    jmethodID mid = nullptr;
    if (static_method)
        mid = env->GetStaticMethodID(cls, methodName, methodSiganture);
    else
        mid = env->GetMethodID(cls, methodName, methodSiganture);
    if (mid == nullptr)
        return getPendingExceptionAndClear(env);
    *out = mid;
    assert(*out != nullptr);
    return nullptr;
}

// NOLINTNEXTLINE
jthrowable invokeObjectMethod(JNIEnv *env, jvalue *retval, jobject instObj, jmethodID mid, const char *methSignature, ...)
{
    va_list args;
    jthrowable jthr;
    const char *str;
    char return_type;

    str = methSignature;
    while (*str != ')') str++;
    str++;
    return_type = *str;
    va_start(args, methSignature);
    if (return_type == JOBJECT || return_type == JARRAYOBJECT) {
        jobject jobj = nullptr;
        jobj = env->CallObjectMethodV(instObj, mid, args);
        retval->l = jobj;
    }
    else if (return_type == JVOID) {
        env->CallVoidMethodV(instObj, mid, args);
    }
    else if (return_type == JBOOLEAN) {
        jboolean jbool = 0;
        jbool = env->CallBooleanMethodV(instObj, mid, args);
        retval->z = jbool;
    }
    else if (return_type == JSHORT) {
        jshort js = 0;
        js = env->CallShortMethodV(instObj, mid, args);
        retval->s = js;
    }
    else if (return_type == JLONG) {
        jlong jl = -1;
        jl = env->CallLongMethodV(instObj, mid, args);
        retval->j = jl;
    }
    else if (return_type == JINT) {
        jint ji = -1;
        ji = env->CallIntMethodV(instObj, mid, args);
        retval->i = ji;
    }
    va_end(args);

    jthr = env->ExceptionOccurred();
    if (jthr) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return jthr;
    }
    return nullptr;
}

// NOLINTNEXTLINE
jthrowable invokeStaticMethod(JNIEnv * env, jvalue * retval, jclass cls, jmethodID mid, const char * methSignature, ...)
{
    va_list args;
    jthrowable jthr;
    const char *str;
    char return_type;

    str = methSignature;
    while (*str != ')') str++;
    str++;
    return_type = *str;
    va_start(args, methSignature);
    if (return_type == JOBJECT || return_type == JARRAYOBJECT) {
        jobject jobj = nullptr;
        jobj = env->CallStaticObjectMethodV(cls, mid, args);
        retval->l = jobj;
    }
    else if (return_type == JVOID) {
        env->CallStaticVoidMethodV(cls, mid, args);
    }
    else if (return_type == JBOOLEAN) {
        jboolean jbool = 0;
        jbool = env->CallStaticBooleanMethodV(cls, mid, args);
        retval->z = jbool;
    }
    else if (return_type == JSHORT) {
        jshort js = 0;
        js = env->CallStaticShortMethodV(cls, mid, args);
        retval->s = js;
    }
    else if (return_type == JLONG) {
        jlong jl = -1;
        jl = env->CallStaticLongMethodV(cls, mid, args);
        retval->j = jl;
    }
    else if (return_type == JINT) {
        jint ji = -1;
        ji = env->CallStaticIntMethodV(cls, mid, args);
        retval->i = ji;
    }
    va_end(args);

    jthr = env->ExceptionOccurred();
    if (jthr) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return jthr;
    }
    return nullptr;
}

std::string jstringToStr(JNIEnv * env, jstring jstr)
{
    const char * chars = env->GetStringUTFChars(jstr, nullptr);
    std::string res = chars;
    env->ReleaseStringUTFChars(jstr, chars);
    return res;
}

std::string jbyteArrayToStr(JNIEnv * env, jbyteArray obj)
{
    jbyte * bytes = env->GetByteArrayElements(obj, nullptr);
    jsize length = env->GetArrayLength(obj);
    std::string result(reinterpret_cast<char *>(bytes), length);
    env->ReleaseByteArrayElements(obj, bytes, JNI_ABORT);
    return result;
}

std::string getExceptionSummary(JNIEnv *env, jthrowable jthr)
{
    jclass execption_clazz = env->GetObjectClass(jthr);
    jmethodID method_get_message;
    const char * signature = "()Ljava/lang/String;";
    findMethodId(env, &method_get_message, execption_clazz, "getMessage", signature, false);

    jvalue return_val;
    invokeObjectMethod(env, &return_val, jthr, method_get_message, signature);
    jstring jstr = static_cast<jstring>(return_val.l);
    JNILocalRefWrapper guard(env, jstr);

    return jstringToStr(env, jstr);
}

}

}
