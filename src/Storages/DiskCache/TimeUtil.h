#pragma once

#include <chrono>


namespace DB::HybridCache
{
inline std::chrono::nanoseconds getSteadyClock()
{
    auto dur = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(dur);
}

inline std::chrono::seconds getSteadyClockSeconds()
{
    auto dur = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::seconds>(dur);
}

inline std::chrono::microseconds toMicros(std::chrono::nanoseconds t)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(t);
}

template <typename T>
inline std::chrono::milliseconds toMillis(T t)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(t);
}
}
