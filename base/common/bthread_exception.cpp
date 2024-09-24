#pragma clang diagnostic ignored "-Wunknown-warning-option"
#pragma clang diagnostic ignored "-Wreserved-identifier"
#include <common/ThreadLocal.h>

namespace {
struct __cxa_eh_globals {
    void *   caughtExceptions;
    unsigned int uncaughtExceptions;
};
}

namespace __cxxabiv1 {

namespace {
    __cxa_eh_globals * __globals () {
        static ThreadLocalManaged<__cxa_eh_globals> eh_globals;
        return eh_globals.get();
        }
    }

extern "C" {
    __cxa_eh_globals * __cxa_get_globals      () { return __globals (); }
    __cxa_eh_globals * __cxa_get_globals_fast () { return __globals (); }
    }
}
