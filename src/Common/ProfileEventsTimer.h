#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace DB
{

class ProfileEventsTimer
{
public:
    explicit ProfileEventsTimer(ProfileEvents::Event event_, UInt64 divide_ = 1000): divide(divide_),
        event(event_) {}

    ~ProfileEventsTimer()
    {
        ProfileEvents::increment(event, watch.elapsedNanoseconds() / divide);
    }

private:
    UInt64 divide;
    Stopwatch watch;
    ProfileEvents::Event event;
};

}
