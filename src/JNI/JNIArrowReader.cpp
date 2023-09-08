#include "JNIArrowReader.h"

#include "JNIByteCreatable.h"
#include "JNIHelper.h"

#include <cassert>
#include <iostream>
#include <system_error>
#include <JNIArrowStream.h>

namespace DB
{

static void print_err(int errcode, struct ArrowArrayStream* stream)
{
   // Print stream error
   const char* errdesc = stream->get_last_error(stream);
   if (errdesc != nullptr) {
      fputs(errdesc, stderr);
   } else {
      fputs(strerror(errcode), stderr);
   }
   // Release stream and abort
   stream->release(stream);
}

JNIArrowReader::JNIArrowReader(const std::string & full_classname, const std::string & pb_message)
    : JNIByteCreatable(full_classname, pb_message)
{
    JNIEnv *env = JNIHelper::instance().getJNIEnv();
    method_init_stream = env->GetMethodID(jni_cls, "initStream", "(J)V");
    assert(method_init_stream);
}

JNIArrowReader::~JNIArrowReader()
{
    // schema.release(&schema);
    // stream.release(&stream);
}

void JNIArrowReader::initStream()
{
    JNIEnv * env = JNIHelper::instance().getJNIEnv();
    env->CallObjectMethod(jni_obj, method_init_stream, reinterpret_cast<jlong>(&stream));
    if (env->ExceptionCheck())
    {
        env->ExceptionDescribe();
        return;
    }
    int errcode = stream.get_schema(&stream, &schema);

    if (env->ExceptionCheck())
    {
        env->ExceptionDescribe();
    }

    assert(errcode == 0);
    if (errcode != 0)
    {
        print_err(errcode, &stream);
        abort();
    }
}

bool JNIArrowReader::next(ArrowArray & chunk)
{
    int errcode = stream.get_next(&stream, &chunk);
    if (errcode != 0)
    {
        /// handle error
        print_err(errcode, &stream);
        return false;
    }

    if (chunk.release == nullptr)
    {
        /// stream ends
        return false;
    }

    return true;
}

}
