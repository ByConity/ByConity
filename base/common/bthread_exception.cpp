#pragma clang diagnostic ignored "-Wunknown-warning-option"
#pragma clang diagnostic ignored "-Wreserved-identifier"
#include <common/ThreadLocal.h>

template<class T>
class ThreadLocalManagedUntracked : public ThreadLocalManagedBase<T, ThreadLocalManagedUntracked<T>> {
public:
    static void *create() {
        void * p = malloc(sizeof(T));
        return new (p) T();
    }
};

namespace {
struct __cxa_eh_globals {
    void *   caughtExceptions;
    unsigned int uncaughtExceptions;
};
}

namespace __cxxabiv1 {

namespace {
    __cxa_eh_globals * __globals () {
        static ThreadLocalManagedUntracked<__cxa_eh_globals> eh_globals;
        return eh_globals.get();
        }
    }

extern "C" {
    __cxa_eh_globals * __cxa_get_globals      () { return __globals (); }
    __cxa_eh_globals * __cxa_get_globals_fast () { return __globals (); }
    }
}
