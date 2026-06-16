#pragma clang diagnostic ignored "-Wunknown-warning-option"
#pragma clang diagnostic ignored "-Wreserved-identifier"
#include <common/ThreadLocal.h>

template<class T>
class ThreadLocalManagedUntracked : public ThreadLocalManagedBase<T, ThreadLocalManagedUntracked<T>> {
public:
    static void *create() {
        return calloc(1, sizeof(T));
    }
};

namespace {
    struct __cxa_eh_globals {
        void *   caughtExceptions;
        unsigned int uncaughtExceptions;
    };

    pthread_key_t key_;
    pthread_once_t flag_ = PTHREAD_ONCE_INIT;

    __cxa_eh_globals * __globals () {
        static ThreadLocalManagedUntracked<__cxa_eh_globals> eh_globals;
        return eh_globals.get();
    }

    void destruct_(void *p) {
        free(p);
        if (0 != pthread_setspecific(key_, NULL))
            abort();
    }

    void construct_() {
        if (0 != pthread_key_create(&key_, destruct_))
            abort();
    }
}

namespace bthread {
    class TaskGroup;
    extern __thread TaskGroup* tls_task_group;
};

extern "C" {
    __cxa_eh_globals * __cxa_get_globals_fast () {
        if (bthread::tls_task_group)
            return __globals();

        // First time through, create the key.
        if (0 != pthread_once(&flag_, construct_))
            abort();
        return static_cast<__cxa_eh_globals*>(pthread_getspecific(key_));
    }

    __cxa_eh_globals * __cxa_get_globals() {
        if (bthread::tls_task_group)
            return __globals();

        // Try to get the globals for this thread
        __cxa_eh_globals *retVal = __cxa_get_globals_fast();

        // If this is the first time we've been asked for these globals, create them
        if (NULL == retVal) {
            retVal = static_cast<__cxa_eh_globals*>(calloc(1, sizeof(__cxa_eh_globals)));
            if (NULL == retVal)
                abort();
            if (0 != pthread_setspecific(key_, retVal))
                abort();
        }
        return retVal;
    }
}
