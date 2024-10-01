#pragma once

#include <folly/container/F14Map.h>
#include <folly/fibers/TimedMutex.h>

#include <Storages/DiskCache/Buffer.h>
#include <common/StringRef.h>

namespace DB
{
#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
// 64 bytes on x86-64 │ L1_CACHE_BYTES │ L1_CACHE_SHIFT │ __cacheline_aligned │ ...
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

using folly::fibers::TimedMutex;

class alignas(hardware_destructive_interference_size) InFlightPuts
{
    using LockGuard = std::lock_guard<TimedMutex>;
    using UniqueLock = std::unique_lock<TimedMutex>;

public:
    class PutToken;
    PutToken tryAcquireToken(StringRef key)
    {
        UniqueLock l(mutex, std::try_to_lock);
        if (!l.owns_lock())
            return PutToken{};

        auto ret = keys.emplace(key, true);
        if (ret.second)
            return PutToken{key, *this};

        return PutToken{};
    }

    void invalidateToken(StringRef key)
    {
        LockGuard l(mutex);
        auto it = keys.find(key);
        if (it != keys.end())
            it->second = false;
    }

    class PutToken
    {
    public:
        PutToken() noexcept = default;
        ~PutToken()
        {
            if (puts)
                puts->removeToken(key);
        }

        PutToken(const PutToken &) = delete;
        PutToken & operator=(const PutToken &) = delete;

        PutToken(PutToken && other) noexcept : key(other.key), puts(other.puts) { other.reset(); }

        PutToken & operator=(PutToken && other) noexcept
        {
            if (this != &other)
            {
                this->~PutToken();
                new (this) PutToken(std::move(other));
            }
            return *this;
        }

        bool isValid() const noexcept { return puts != nullptr; }

        template <typename F>
        bool executeIfValid(F && fn)
        {
            if (isValid() && puts->executeIfValid(key, std::forward<decltype(fn)>(fn)))
            {
                reset();
                return true;
            }
            return false;
        }

        StringRef getKey() const { return key; }

    private:
        void reset() noexcept { puts = nullptr; }

        friend InFlightPuts;
        PutToken(StringRef key_, InFlightPuts & puts_) : key{key_}, puts(&puts_) { }

        StringRef key;
        InFlightPuts * puts{nullptr};
    };

private:
    template <typename F>
    bool executeIfValid(StringRef key, F && fn)
    {
        LockGuard l(mutex);
        auto it = keys.find(key);
        const bool valid = it != keys.end() && it->second;
        if (valid)
        {
            fn();
            keys.erase(it);
            return true;
        }
        return false;
    }

    void removeToken(StringRef key)
    {
        LockGuard l(mutex);
        auto res = keys.erase(key);
        chassert(res == 1u);
    }

    folly::F14FastMap<StringRef, bool> keys;
    TimedMutex mutex;
};
}
