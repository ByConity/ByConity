#include "JNIHelper.h"

#include <cassert>
#include <mutex>
#include <string>
#include <vector>

static constexpr auto CLASSPATH = "CLASSPATH";
static constexpr auto OPT_CLASSPATH = "-Djava.class.path=";
static constexpr auto JVM_ARGS = "LIBHDFS_OPTS";

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
        std::vector<std::string> options;

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

// find a jclass and return a global jclass ref
static jclass jniFindClass(JNIEnv *env, const char *className)
{
    jclass l_class = env->FindClass(className);
    assert(l_class != nullptr);
    jclass g_class = static_cast<jclass>(env->NewGlobalRef(l_class));
    env->DeleteLocalRef(l_class);
    return g_class;
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

    // assert(cached_env != nullptr);
    static JNIHelper helper;
    return helper;
}

void JNIHelper::init() {
    object_class = jniFindClass(cached_env, "java/lang/Object");
    object_array_class = jniFindClass(cached_env, "[Ljava/lang/Object;");
    string_class = jniFindClass(cached_env, "java/lang/String");
    throwable_class = jniFindClass(cached_env, "java/lang/Throwable");
    arrays_class = jniFindClass(cached_env, "java/util/Arrays");
    list_class = jniFindClass(cached_env, "java/util/List");
    hashmap_class = jniFindClass(cached_env, "java/util/HashMap");
}

}
