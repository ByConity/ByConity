#pragma once
#include <bthread/bthread.h>
#include <compare>
#include <cstdint>
#include <memory>

template<class T>
concept Integral = std::is_integral_v<T>;

template<class T>
concept Pointer = std::is_pointer_v<T>;

template<class T>
class ThreadLocal {
public:
    ThreadLocal() noexcept {
        bthread_key_create(&key, nullptr);
    }

    ~ThreadLocal() noexcept {
        bthread_key_delete(key);
    }

    bool operator==(const ThreadLocal & rhs) const noexcept {
        return get() == rhs.get();
    }

    auto &operator=(T t) const noexcept {
        set(reinterpret_cast<void *>(t));
        return *this;
    }

    auto &operator=(void *p) const noexcept {
        set(p);
        return *this;
    }

    operator T() const noexcept {
        if constexpr (std::is_pointer_v<T>)
            return reinterpret_cast<T>(reinterpret_cast<uintptr_t>(get()));
        else
            return static_cast<T>(reinterpret_cast<uintptr_t>(get()));
    }

    // prefix
    auto operator++() const noexcept requires Integral<T> {
        T val = operator T() + 1;
        *this = val;
        return val;
    }

    auto operator--() const noexcept requires Integral<T> {
        T val = operator T() - 1;
        *this = val;
        return val;
    }

    // postfix
    T operator++(int) const noexcept requires Integral<T> {
        T val = operator T();
        *this = val + 1;
        return val;
    }

    T operator--(int) const noexcept requires Integral<T> {
        T val = operator T();
        *this = val - 1;
        return val;
    }

    T operator%(int d) const noexcept requires Integral<T> {
        return operator T() % d;
    }

    T operator->() const noexcept requires Pointer<T> {
        return operator T();
    }

    auto &operator*() const noexcept requires Pointer<T> {
        return *operator T();
    }

    auto &operator=(std::nullptr_t) const noexcept requires Pointer<T> {
        set(nullptr);
        return *this;
    }

protected:
    void* get() const noexcept {
       return bthread_getspecific(key);
    }

    void set(void *val) const noexcept {
       bthread_setspecific(key, val);
    }

private:
    bthread_key_t key;
};

template<class T, class I>
class ThreadLocalManagedBase {
public:
    ThreadLocalManagedBase() noexcept {
        bthread_key_create(&key, [](void * obj) {
            static_cast<T *>(obj)->~T();
            free(obj);
        });
    }

    ~ThreadLocalManagedBase() noexcept { bthread_key_delete(key); }

    explicit operator bool() {
        return !!bthread_getspecific(key);
    }

    T *operator->() const noexcept {
        return get();
    }

    T *get() const noexcept {
        void *p = bthread_getspecific(key);
        if (!p) {
            p = I::create();
            bthread_setspecific(key, p);
        }

        return static_cast<T *>(p);
    }

private:
    bthread_key_t key;
};
