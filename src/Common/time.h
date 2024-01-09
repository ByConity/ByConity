#pragma once

inline UInt64 time_in_nanoseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_milliseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

inline std::chrono::nanoseconds timespec_to_duration(timespec ts)
{
    auto duration = std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec};
    return duration_cast<std::chrono::nanoseconds>(duration);
}

inline std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> timespec_to_timepoint(timespec ts)
{
    return std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>{
        std::chrono::duration_cast<std::chrono::system_clock::duration>(timespec_to_duration(ts))};
}
