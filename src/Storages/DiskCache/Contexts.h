#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <semaphore>
#include <unordered_map>
#include <utility>
#include <stdint.h>

#include <folly/container/F14Map.h>
#include <folly/fibers/TimedMutex.h>

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/InFlightPuts.h>
#include <Common/Exception.h>
#include <common/StringRef.h>
#include <common/defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_ERROR_CODE;
}

class PutCtx
{
public:
    PutCtx(StringRef key_, std::shared_ptr<void> item_) : item{item_}, key{key_.toString()} { }

    StringRef getKey() const { return StringRef{key.data(), key.length()}; }

    static StringRef type() { return "put ctx"; }

private:
    std::shared_ptr<void> item;
    std::string key;
};

using folly::fibers::TimedMutex;

class alignas(hardware_destructive_interference_size) TombStones
{
public:
    class Guard;

    Guard add(StringRef key)
    {
        std::lock_guard<TimedMutex> guard{mutex};
        auto it = keys.find(key.toString());

        if (it == keys.end())
            it = keys.insert(std::make_pair(key.toString(), 0)).first;

        ++it->second;
        return Guard(it->first, *this);
    }

    bool isPresent(StringRef key)
    {
        std::lock_guard<TimedMutex> guard{mutex};
        return keys.count(key.toString()) != 0;
    }

    class Guard
    {
    public:
        Guard() = default;
        ~Guard()
        {
            if (tombstones)
            {
                tombstones->remove(key);
                tombstones = nullptr;
            }
        }

        Guard(const Guard &) = delete;
        Guard & operator=(const Guard &) = delete;

        Guard(Guard && other) noexcept : key{other.key.data, other.key.size}, tombstones(other.tombstones) { other.tombstones = nullptr; }
        Guard & operator=(Guard && other) noexcept
        {
            if (this != &other)
            {
                this->~Guard();
                new (this) Guard(std::move(other));
            }
            return *this;
        }

        StringRef getKey() const noexcept { return key; }
        explicit operator bool() const noexcept { return tombstones != nullptr; }

    private:
        friend TombStones;
        Guard(StringRef key_, TombStones & t) noexcept : key{key_}, tombstones(&t) { }

        StringRef key;
        TombStones * tombstones{nullptr};
    };

private:
    void remove(StringRef key)
    {
        std::lock_guard<TimedMutex> guard{mutex};
        auto it = keys.find(key.toString());
        if (it == keys.end() || it->second == 0)
            throw Exception(
                ErrorCodes::UNEXPECTED_ERROR_CODE,
                "Invalid state. Key: {}. State: {}",
                key,
                it == keys.end() ? "not exist" : "exists, but count is 0");

        if (--(it->second) == 0)
            keys.erase(it);
    }
    TimedMutex mutex;
    folly::F14NodeMap<std::string, UInt64> keys;
};

class DelCtx
{
public:
    DelCtx(StringRef, TombStones::Guard tombstone_) : tombstone{std::move(tombstone_)} { }

    StringRef getKey() const { return tombstone.getKey(); }

    static StringRef type() { return "del ctx"; }

private:
    TombStones::Guard tombstone;
};

template <typename T>
class ContextMap
{
public:
    template <typename... Args>
    T & createContext(StringRef key, Args &&... args)
    {
        auto ctx = std::make_unique<T>(key, std::forward<Args>(args)...);
        uintptr_t ctx_key = reinterpret_cast<uintptr_t>(ctx.get());
        std::lock_guard<TimedMutex> guard{mutex};
        auto it = map.find(ctx_key);
        if (it != map.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate {} for key {}", T::type(), key);

        auto [kv, inserted] = map.emplace(ctx_key, std::move(ctx));
        chassert(inserted);
        return *kv->second;
    }

    bool hasContexts() const
    {
        std::lock_guard<TimedMutex> guard{mutex};
        return map.size() != 0;
    }

    void destroyContext(const T & ctx)
    {
        uintptr_t ctx_key = reinterpret_cast<uintptr_t>(&ctx);
        std::lock_guard<TimedMutex> guard{mutex};
        size_t num_removed = map.erase(ctx_key);
        if (num_removed != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid {} state for {}, found {}", T::type(), ctx.getKey(), num_removed);
    }

private:
    folly::F14FastMap<uintptr_t, std::unique_ptr<T>> map;
    mutable TimedMutex mutex;
};

using PutContexts = ContextMap<PutCtx>;
using DelContexts = ContextMap<DelCtx>;


struct WaitContext
{
    explicit WaitContext() = default;
    ~WaitContext() = default;

    std::shared_ptr<void> get() const noexcept
    {
        wait();
        chassert(isReady());
        return obj;
    }

    void wait() const noexcept
    {
        if (isReady())
            return;
        sem.acquire();
        chassert(isReady());
    }

    void set(std::shared_ptr<void> obj_)
    {
        chassert(!isReady());
        obj = obj_;
        ready.store(true, std::memory_order_release);
        sem.release();
    }

    bool isReady() const noexcept { return ready.load(std::memory_order_acquire); }

private:
    mutable std::binary_semaphore sem{0};
    std::shared_ptr<void> obj{nullptr};
    std::atomic<bool> ready{false};
};

struct GetCtx
{
    const std::string key;
    std::vector<std::shared_ptr<WaitContext>> waiters;
    std::shared_ptr<void> obj;
    bool valid;

    GetCtx(StringRef key_, std::shared_ptr<WaitContext> ctx, std::shared_ptr<void> obj_) : key(key_.toString()), obj(obj_), valid(true)
    {
        addWaiter(ctx);
    }

    ~GetCtx() { wakeUpWaiters(); }

    void addWaiter(std::shared_ptr<WaitContext> waiter)
    {
        chassert(waiter);
        waiters.push_back(std::move(waiter));
    }

    void wakeUpWaiters()
    {
        if (!isValid())
            return;

        for (auto & w : waiters)
        {
            w->set(obj);
        }
    }

    StringRef getKey() const { return StringRef{key.data(), key.length()}; }

    void invalidate() { valid = false; }

    bool isValid() const { return valid; }
};
}
