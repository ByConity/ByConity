#include <stdio.h>
#include <stdlib.h>

#include "jni.h"

// about SUNWprivate_1.1 ref: https://github.com/joekoolade/JOE/blob/26f1d1cf1b7d253ee1a9752d2944cc38c52f4ced/tools/bootloader/libjvm.exp#L28
// This file is the mock file that helps us get BE started even if we don't have a JAVA_HOME configuration

#define PRINT_ERROR_MSG() \
    fprintf(stderr, "This is a mock JVM implementation. Please set correct search path for 'libjvm.so' via 'LD_LIBRARY_PATH'\n")

_JNI_IMPORT_OR_EXPORT_ jint JNICALL JNI_GetDefaultJavaVMInitArgs(void * /*args*/)
{
    PRINT_ERROR_MSG();
    return 1;
}

_JNI_IMPORT_OR_EXPORT_ jint JNICALL JNI_CreateJavaVM(JavaVM ** /*pvm*/, void ** /*penv*/, void * /*args*/)
{
    PRINT_ERROR_MSG();
    return 1;
}

_JNI_IMPORT_OR_EXPORT_ jint JNICALL JNI_GetCreatedJavaVMs(JavaVM ** /*vm*/, jsize /*sz*/, jsize * /*output*/)
{
    PRINT_ERROR_MSG();
    return 1;
}

/* Defined by native libraries. */
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM * /*vm*/, void * /*reserved*/)
{
    PRINT_ERROR_MSG();
    return 1;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM * /*vm*/, void * /*reserved*/)
{
    PRINT_ERROR_MSG();
}
