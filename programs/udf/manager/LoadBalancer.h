#pragma once
#include <atomic>
#include <memory>
#include <vector>

#include <unistd.h>
#include <linux/futex.h>
#include <sys/syscall.h>

#include "IClient.h"

#ifndef unlikely
#define unlikely(x)     __builtin_expect((x), 0)
#endif

class LoadBalancer
{
public:
    explicit LoadBalancer(std::vector<std::unique_ptr<IClient>> &&clients_)
    : clients(std::move(clients_)) {
        int cnt = clients.size();

        longcnt = (cnt + sizeof(long) * 8 - 1) / sizeof(long) / 8;
        if (!longcnt)
            longcnt = 1;

        freelist = std::make_unique<unsigned long[]>(longcnt);
        offsets = std::make_unique<int[]>(longcnt);

        for (int i = 1; i < longcnt; i++)
            offsets[i] = sizeof(long) * 8 * i;

        /* set free bit */
        for (int i = 0; i < cnt; i++)
            atomic_set_bit(i, freelist.get());

        freecnt = cnt;
    }

    IClient *SelectServer(void) {
        int idx;

        while (true) {
            idx = FindFirstFreeServerIndex();

            if (unlikely(idx == -1)) {
                /* all servers are busy, let's wait for the futex */
                syscall(SYS_futex, &freecnt, FUTEX_WAIT_PRIVATE, 0, nullptr);
                continue;
            }

            /* test and clear free bit */
            if (unlikely(!atomic_test_and_clear_bit(idx, freelist.get()))) {
                /* bit cleared already, retry */
                continue;
            }
            break;
        }

        /* atomic_test_and_clear_bit() is a barrier,
         * memory order can be relaxed */
        freecnt.fetch_sub(1, std::memory_order_relaxed);
        return clients[idx].get();
    }

    void Feedback(const IClient *client)
    {
        if (unlikely(!client))
            return;

        Feedback(client->GetOffset());
    }

    void Feedback(int offset)
    {
        /* set_bit() is a barrier, memory order can be relaxed */
        freecnt.fetch_add(1, std::memory_order_relaxed);
        /* set free bit */
        atomic_set_bit(offset, freelist.get());
        /* wake up one client */
        syscall(SYS_futex, &freecnt, FUTEX_WAKE_PRIVATE, 1);
    }


    /* TODO: to be replaced by atomic_fetch_or_explicit() once GCC 12 able
     * to generate lock btsq instead of lock cmpxchg */
    static inline void atomic_set_bit(long nr, volatile unsigned long *addr) {
        asm volatile("lock; btsq %1,%0"
                     : "+m" (*addr)
                     : "Ir" (nr)
                     : "memory");
    }

    /* TODO: to be replaced by atomic_fetch_and_explicit() once GCC 12 able
     * to generate lock btrq instead of lock cmpxchg */
    static inline bool atomic_test_and_clear_bit(long nr,
                                                 volatile unsigned long *addr) {
        unsigned char oldbit;

        asm volatile("lock; btrq %2,%1\n\tsetc %0"
                    : "=qm" (oldbit), "+m" (*addr)
                    : "Ir" (nr) : "memory");
        return oldbit;
    }

    int FindFirstFreeServerIndex() {
        for (int i = 0; i < longcnt; i++) {
            int lsb = __builtin_ffsl(freelist[i]);

            if (!lsb)
                continue;

            return offsets[i] + lsb - 1;
        }
        return -1;
    }

    int GetFreeCount() { return freecnt; }

private:
    std::vector<std::unique_ptr<IClient>> clients;
    /* each long can store 64 servers */
    std::unique_ptr<unsigned long[]> freelist; /* free server list */
    std::unique_ptr<int []> offsets; /* bit offset of each long */
    int longcnt;
    std::atomic_int freecnt;
};
